import type { Store, AssistantRecord, ThreadRecord, RunRecord, CheckpointRecord } from "./db/index.ts";
import { Overwrite } from "@langchain/langgraph";
import { MULTITASK_STRATEGIES, ThreadBusyError, type CancelAction, type GraphStateSnapshot,
  type MultitaskStrategy } from "./engine/index.ts";
import { describeGraph } from "./engine/introspection.ts";
import type { PlatformAdapter, JsonRecord, Assistant, Thread, Run, ThreadState, Checkpoint } from "./api/types.ts";
import { ApiError } from "./api/types.ts";
import { currentUser } from "./auth.ts";
import { matchesAuthorizationFilter } from "./authz.ts";
import type { AssistantVersionsExtension } from "./extensions/assistant_versions.ts";
import { ThreadPruner } from "./extensions/thread_prune.ts";
import { threadTtlForRequest, type ThreadTtlPolicy } from "./extensions/thread_ttl_config.ts";
import { copyThread } from "./extensions/thread_copy.ts";
import { parseThreadProjection, projectThread } from "./api/thread_projection.ts";

type RuntimeHandle = {
  startRun(input: {
    threadId: string; graphId: string; assistantId?: string;
    input?: unknown; config?: JsonRecord; metadata?: JsonRecord;
    multitaskStrategy?: MultitaskStrategy;
  }): Promise<RunRecord>;
  resumeRun(input: { threadId: string; resume: unknown; graphId?: string;
    assistantId?: string; config?: JsonRecord; metadata?: JsonRecord;
    update?: JsonRecord; goto?: string | string[]; multitaskStrategy?: MultitaskStrategy }): Promise<RunRecord>;
  cancelRun?(runId: string, action?: CancelAction): Promise<RunRecord | null>;
  getRun(id: string): Promise<RunRecord | null>;
  stream(id: string, options?: { after?: number; pollMs?: number }): AsyncIterable<{ event: string; data: unknown; id: string }>;
  updateState(threadId: string, update: JsonRecord, asNode?: string, checkpointId?: string):
    Promise<CheckpointRecord & { nativeCheckpointId?: string }>;
  getGraph?(id: string): unknown;
  getGraphState?(threadId: string, checkpointId?: string, graphId?: string): Promise<GraphStateSnapshot | null>;
  getGraphHistory?(threadId: string, limit?: number, graphId?: string): Promise<GraphStateSnapshot[]>;
  captureTraceContext?(): { traceparent?: string; tracestate?: string } | undefined;
};

const BUSY_MESSAGE = "Thread is already running a task. Wait for it to finish or choose a different multitask strategy.";
/** LangGraph's server queues by default (since langgraph-api 0.2.130); reject must be asked for. */
function multitaskStrategy(value: unknown): MultitaskStrategy {
  if (value == null) return "enqueue";
  if (MULTITASK_STRATEGIES.includes(value as MultitaskStrategy)) return value as MultitaskStrategy;
  throw new ApiError(422, `Invalid multitask_strategy '${String(value)}'`);
}
function streamModes(value: unknown): string[] {
  let raw = value;
  if (typeof raw === "string") {
    try { raw = raw.startsWith("[") ? JSON.parse(raw) : raw.split(","); } catch { return []; }
  }
  return Array.isArray(raw) ? raw.filter((mode): mode is string => typeof mode === "string").map(mode => mode.trim()) : [];
}
const object = (value: unknown): JsonRecord =>
  value && typeof value === "object" && !Array.isArray(value) ? value as JsonRecord : {};
const number = (value: unknown, fallback: number) => {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : fallback;
};
const apiAssistant = (record: AssistantRecord): Assistant => ({
  assistant_id: record.id, graph_id: record.graphId, name: record.name,
  description: record.description, config: record.config, metadata: record.metadata,
  created_at: record.createdAt, updated_at: record.updatedAt,
});
const apiThread = (record: ThreadRecord): Thread => ({
  thread_id: record.id, status: record.status, metadata: record.metadata,
  created_at: record.createdAt, updated_at: record.updatedAt,
});
const apiRun = (record: RunRecord): Run => ({
  run_id: record.id, thread_id: record.threadId, assistant_id: record.assistantId ?? record.graphId,
  // LangGraph reports a cancelled or superseded run as interrupted.
  status: record.status === "cancelled" ? "interrupted" : record.status,
  metadata: Object.fromEntries(Object.entries(record.metadata).filter(([key]) => !key.startsWith("__"))),
  created_at: record.createdAt, updated_at: record.updatedAt,
});
const apiState = (record: CheckpointRecord | null, threadId: string): ThreadState => ({
  values: record?.values ?? {}, next: record?.next ?? [],
  tasks: record?.tasks.map(object) ?? [], interrupts: record?.interrupts.map(object) ?? [],
  metadata: { step: record?.step ?? 0, source: "loop" },
  checkpoint: record ? { thread_id: threadId, checkpoint_id: record.id, checkpoint_ns: "" } : null,
  parent_checkpoint: record?.parentId
    ? { thread_id: threadId, checkpoint_id: record.parentId, checkpoint_ns: "" } : null,
  created_at: record?.createdAt ?? null,
});
const apiGraphState = (snapshot: GraphStateSnapshot): ThreadState => ({
  values: snapshot.values, next: snapshot.next,
  tasks: snapshot.tasks.map(object), interrupts: snapshot.interrupts.map(object),
  metadata: snapshot.metadata,
  checkpoint: snapshot.config as Checkpoint | null,
  parent_checkpoint: snapshot.parentConfig as Checkpoint | null,
  created_at: snapshot.createdAt,
});

function owner(): string | undefined { return currentUser.getStore()?.identity; }
function visible(record: ThreadRecord | null): boolean {
  const identity = owner();
  if (!record) return true;
  if (identity && record.metadata._owner && record.metadata._owner !== identity) return false;
  return matchesAuthorizationFilter("threads", apiThread(record));
}
function ensureVisible(record: ThreadRecord | null): ThreadRecord | null {
  if (!visible(record)) throw new ApiError(403, "Thread access denied");
  return record;
}
/** Thread ownership is assigned by the server; a client-supplied `_owner` is ignored. */
function clientMetadata(value: unknown): JsonRecord {
  const { _owner: _ignored, ...metadata } = object(value);
  return metadata;
}
/** `__`-prefixed run metadata carries runtime instructions such as resume commands. */
function publicEntries(value: unknown): JsonRecord {
  return Object.fromEntries(Object.entries(object(value)).filter(([key]) => !key.startsWith("__")));
}
function matchesMetadata(value: JsonRecord, filter: JsonRecord): boolean {
  return Object.entries(filter).every(([key, expected]) => value[key] === expected);
}
function matchesValues(value: unknown, filter: unknown): boolean {
  if (filter && typeof filter === "object" && !Array.isArray(filter)) {
    if (!value || typeof value !== "object" || Array.isArray(value)) return false;
    return Object.entries(filter).every(([key, expected]) =>
      matchesValues((value as JsonRecord)[key], expected));
  }
  return JSON.stringify(value) === JSON.stringify(filter);
}
const threadSortFields = new Set(["thread_id", "status", "created_at", "updated_at", "state_updated_at"]);

export async function seedDefaultAssistants(store: Store, graphIds: string[]): Promise<void> {
  for (const id of graphIds) {
    if (!await store.getAssistant(id)) await store.createAssistant({ id, graphId: id, name: id });
  }
}

export function createPlatformAdapter(
  runtime: RuntimeHandle, store: Store, graphIds: string[],
  extensions: { store?: PlatformAdapter["store"]; crons?: PlatformAdapter["crons"];
    versions?: AssistantVersionsExtension; pruner?: ThreadPruner; ttl?: ThreadTtlPolicy | null } = {},
): PlatformAdapter {
  const pruner = extensions.pruner ?? new ThreadPruner(store);
  const ttlPolicy = extensions.ttl ?? null;
  async function getThread(id: string) { return ensureVisible(await store.getThread(id)); }
  async function threadWithValues(row: ThreadRecord, knownState?: CheckpointRecord | null): Promise<Thread> {
    const state = knownState === undefined ? await store.getState(row.id) : knownState;
    const snapshot = state ? await runtime.getGraphState?.(row.id) : null;
    const interruptTasks = snapshot?.tasks ?? state?.tasks ?? [];
    const interrupts: JsonRecord = {};
    for (const [index, task] of interruptTasks.entries()) {
      const entry = object(task);
      if (!Array.isArray(entry.interrupts) || !entry.interrupts.length) continue;
      const key = typeof entry.id === "string" ? entry.id
        : typeof entry.name === "string" ? entry.name : String(index);
      interrupts[key] = entry.interrupts;
    }
    if (!Object.keys(interrupts).length && (snapshot?.interrupts ?? state?.interrupts)?.length) {
      interrupts["0"] = snapshot?.interrupts ?? state?.interrupts ?? [];
    }
    const checkpoint = snapshot?.config ?? (state
      ? { thread_id: row.id, checkpoint_id: state.id, checkpoint_ns: "" } : null);
    return { ...apiThread(row), values: snapshot?.values ?? state?.values ?? {},
      state_updated_at: state?.createdAt ?? row.createdAt,
      config: checkpoint ? { configurable: checkpoint } : {}, interrupts };
  }
  async function getRun(id: string, threadId: string | null) {
    const result = await runtime.getRun(id);
    if (!result || (threadId && result.threadId !== threadId)) return null;
    if (!await getThread(result.threadId)) return null;
    return result;
  }
  async function getAssistant(id: string) {
    return await store.getAssistant(id) ?? (graphIds.includes(id)
      ? await store.createAssistant({ id, graphId: id, name: id }) : null);
  }
  function assistantVisible(record: AssistantRecord): boolean {
    return matchesAuthorizationFilter("assistants", apiAssistant(record));
  }
  async function visibleAssistant(id: string): Promise<AssistantRecord | null> {
    const row = await getAssistant(id);
    if (row && !assistantVisible(row)) throw new ApiError(403, "Assistant access denied");
    return row;
  }

  return {
    store: extensions.store,
    crons: extensions.crons,
    assistants: {
      async create(payload) {
        const graphId = String(payload.graph_id);
        if (!graphIds.includes(graphId)) throw new ApiError(422, `Unknown graph: ${graphId}`);
        if (typeof payload.assistant_id === "string") {
          const existing = await store.getAssistant(payload.assistant_id);
          if (existing && payload.if_exists !== "do_nothing") {
            throw new ApiError(409, `Assistant '${payload.assistant_id}' already exists`);
          }
          if (existing) {
            if (!assistantVisible(existing)) throw new ApiError(403, "Assistant access denied");
            const assistant = apiAssistant(existing);
            return extensions.versions ? extensions.versions.decorate(assistant) : assistant;
          }
        }
        const created = await store.createAssistant({
          id: typeof payload.assistant_id === "string" ? payload.assistant_id : undefined,
          graphId, name: typeof payload.name === "string" ? payload.name : graphId,
          description: typeof payload.description === "string" ? payload.description : null,
          config: object(payload.config), metadata: object(payload.metadata),
        });
        const assistant = { ...apiAssistant(created), context: object(payload.context) };
        return extensions.versions ? extensions.versions.recordCreated(assistant) : assistant;
      },
      async search(query) {
        const rows = await store.listAssistants(100_000);
        const filtered = rows.filter(row =>
          assistantVisible(row) &&
          (!query.graph_id || row.graphId === query.graph_id) &&
          (!query.name || row.name === query.name) &&
          matchesMetadata(row.metadata, object(query.metadata)),
        );
        const selected = filtered.slice(number(query.offset, 0), number(query.offset, 0) + number(query.limit, 10)).map(apiAssistant);
        return extensions.versions ? Promise.all(selected.map(assistant => extensions.versions!.decorate(assistant))) : selected;
      },
      async get(id) {
        const row = await visibleAssistant(id);
        if (!row) return null;
        const assistant = apiAssistant(row);
        return extensions.versions ? extensions.versions.decorate(assistant) : assistant;
      },
      async update(id, payload) {
        const previous = await visibleAssistant(id);
        if (!previous) return null;
        const row = await store.updateAssistant(id, {
          name: typeof payload.name === "string" ? payload.name : undefined,
          description: typeof payload.description === "string" ? payload.description : undefined,
          config: payload.config === undefined ? undefined : object(payload.config),
          metadata: payload.metadata === undefined ? undefined : object(payload.metadata),
        });
        if (!row) return null;
        const assistant = { ...apiAssistant(row),
          context: payload.context === undefined ? undefined : object(payload.context) };
        return extensions.versions
          ? extensions.versions.recordUpdated(
            await extensions.versions.decorate(apiAssistant(previous)), assistant,
          ) : assistant;
      },
      async delete(id) {
        const existing = await visibleAssistant(id);
        if (!existing) return false;
        await store.deleteAssistant(id);
        await extensions.versions?.delete(id);
        return true;
      },
      versions: extensions.versions ? async (id, query, context) => {
        const row = await visibleAssistant(id);
        if (!row) throw new ApiError(404, `Assistant '${id}' not found`);
        return extensions.versions!.versions(id, query, context);
      } : undefined,
      setLatest: extensions.versions ? async (id, version, context) => {
        if (!await visibleAssistant(id)) return null;
        return extensions.versions!.setLatest(id, version, context);
      } : undefined,
      async graph(id) {
        const assistant = await visibleAssistant(id);
        const graph = assistant && runtime.getGraph?.(assistant.graphId);
        return graph ? { ...describeGraph(graph).graph } : null;
      },
      async schemas(id) {
        const assistant = await visibleAssistant(id);
        const graph = assistant && runtime.getGraph?.(assistant.graphId);
        return graph ? { graph_id: assistant.graphId, ...describeGraph(graph).schemas } : null;
      },
      async subgraphs(id, query) {
        const assistant = await visibleAssistant(id);
        const graph = assistant && runtime.getGraph?.(assistant.graphId);
        if (!graph) return null;
        const subgraphs = describeGraph(graph).subgraphs;
        const namespace = typeof query.namespace === "string" ? query.namespace : undefined;
        return namespace ? subgraphs[namespace] ?? null : subgraphs;
      },
    },
    threads: {
      async create(payload) {
        const metadata = clientMetadata(payload.metadata);
        if (owner()) metadata._owner = owner();
        if (typeof payload.thread_id === "string") {
          const existing = await getThread(payload.thread_id);
          if (existing && payload.if_exists !== "do_nothing") {
            throw new ApiError(409, `Thread '${payload.thread_id}' already exists`);
          }
          if (existing) return threadWithValues(existing);
        }
        const row = await store.createThread({
          id: typeof payload.thread_id === "string" ? payload.thread_id : undefined,
          metadata,
          ttl: threadTtlForRequest(payload.ttl, ttlPolicy, "create") ?? undefined,
        });
        return threadWithValues(row);
      },
      async search(query) {
        const projection = parseThreadProjection(query);
        if (query.ids != null && (!Array.isArray(query.ids) ||
          !query.ids.every(id => typeof id === "string"))) {
          throw new ApiError(422, "ids must be an array of thread IDs");
        }
        if (query.values != null &&
          (typeof query.values !== "object" || Array.isArray(query.values))) {
          throw new ApiError(422, "values must be an object");
        }
        if (query.sort_by != null &&
          (typeof query.sort_by !== "string" || !threadSortFields.has(query.sort_by))) {
          throw new ApiError(422, "Unsupported thread sort_by");
        }
        if (query.sort_order != null && query.sort_order !== "asc" && query.sort_order !== "desc") {
          throw new ApiError(422, "sort_order must be asc or desc");
        }
        const ids = query.ids == null ? null : new Set(query.ids as string[]);
        const values = query.values == null ? null : object(query.values);
        const sortBy = typeof query.sort_by === "string" ? query.sort_by : "created_at";
        const direction = query.sort_order === "asc" ? 1 : -1;
        const rows = await store.listThreads(100_000);
        const filtered = rows.filter(row => visible(row) &&
          (ids === null || ids.has(row.id)) &&
          (!query.status || row.status === query.status) &&
          matchesMetadata(row.metadata, object(query.metadata)));
        const needsState = values !== null || sortBy === "state_updated_at";
        let candidates = needsState
          ? await Promise.all(filtered.map(async row => ({ row, state: await store.getState(row.id) })))
          : filtered.map(row => ({ row, state: undefined as CheckpointRecord | null | undefined }));
        if (values !== null) {
          const matched = await Promise.all(candidates.map(async candidate => matchesValues(
            (await runtime.getGraphState?.(candidate.row.id))?.values ?? candidate.state?.values ?? {}, values)));
          candidates = candidates.filter((_, index) => matched[index]);
        }
        const sortValue = ({ row, state }: (typeof candidates)[number]): string => {
          switch (sortBy) {
            case "thread_id": return row.id;
            case "status": return row.status;
            case "created_at": return row.createdAt;
            case "state_updated_at": return state?.createdAt ?? row.createdAt;
            default: return row.updatedAt;
          }
        };
        candidates.sort((left, right) => {
          const primary = sortValue(left).localeCompare(sortValue(right));
          return primary ? primary * direction : left.row.id.localeCompare(right.row.id);
        });
        const offset = number(query.offset, 0);
        const limit = number(query.limit, 10);
        return Promise.all(candidates.slice(offset, offset + limit).map(async ({ row, state }) =>
          projectThread(await threadWithValues(row, state), projection)));
      },
      async get(id) { const row = await getThread(id); return row ? threadWithValues(row) : null; },
      async update(id, payload) {
        const row = await getThread(id);
        if (!row) return null;
        const updated = await store.updateThread(id, {
          metadata: payload.metadata === undefined ? undefined : { ...row.metadata, ...clientMetadata(payload.metadata) },
          ttl: threadTtlForRequest(payload.ttl, ttlPolicy, "update"),
        });
        return updated ? threadWithValues(updated) : null;
      },
      async delete(id) {
        if (!await getThread(id)) return false;
        await store.deleteThread(id);
        return true;
      },
      async getState(id, checkpoint) {
        if (!await getThread(id)) return null;
        const checkpointId = typeof checkpoint === "string" ? checkpoint : checkpoint?.checkpoint_id;
        if (runtime.getGraphState) {
          const native = await runtime.getGraphState(id, checkpointId);
          if (native) return apiGraphState(native);
        }
        const row = checkpointId ? await store.getCheckpoint(checkpointId) : await store.getState(id);
        return row && row.threadId !== id ? null : apiState(row, id);
      },
      async updateState(id, payload) {
        if (!await getThread(id)) return null;
        if (payload.checkpoint != null &&
          (typeof payload.checkpoint !== "object" || Array.isArray(payload.checkpoint))) {
          throw new ApiError(422, "checkpoint must be an object");
        }
        const checkpoint = object(payload.checkpoint);
        if (checkpoint.thread_id != null && checkpoint.thread_id !== id) {
          throw new ApiError(422, "checkpoint belongs to another thread");
        }
        if (checkpoint.checkpoint_ns != null && checkpoint.checkpoint_ns !== "") {
          throw new ApiError(422, "Only root checkpoint updates are supported");
        }
        const fromObject = checkpoint.checkpoint_id;
        const fromField = payload.checkpoint_id;
        if (fromObject != null && fromField != null && fromObject !== fromField) {
          throw new ApiError(422, "checkpoint and checkpoint_id disagree");
        }
        const checkpointId = fromField ?? fromObject;
        if (checkpointId != null && (typeof checkpointId !== "string" || !checkpointId)) {
          throw new ApiError(422, "checkpoint_id must be a non-empty string");
        }
        const previous = await store.getState(id);
        if (checkpointId && (!previous || !await runtime.getGraphState?.(id, checkpointId))) {
          throw new ApiError(404, `Checkpoint '${checkpointId}' not found`);
        }
        const row: CheckpointRecord & { nativeCheckpointId?: string } = previous
          ? await runtime.updateState(id, object(payload.values),
            typeof payload.as_node === "string" ? payload.as_node : undefined,
            checkpointId as string | undefined)
          : await store.createCheckpoint({
            threadId: id, runId: crypto.randomUUID(), graphId: graphIds[0] ?? "unknown",
            step: 0, values: object(payload.values), next: [], tasks: [], interrupts: [], parentId: null,
          });
        const native = await runtime.getGraphState?.(id, row.nativeCheckpointId);
        return native?.config ? { ...native.config }
          : { thread_id: id, checkpoint_id: row.id, checkpoint_ns: "" };
      },
      async history(id, query) {
        if (!await getThread(id)) return null;
        if (runtime.getGraphHistory) {
          const native = await runtime.getGraphHistory(id, number(query.limit, 10));
          if (native.length) return native.map(apiGraphState);
        }
        return (await store.getHistory(id, number(query.limit, 10))).map(row => apiState(row, id));
      },
      async copy(id) {
        const original = await getThread(id);
        if (!original) return null;
        return threadWithValues(await copyThread(store, original, owner()));
      },
      async prune(payload) {
        const strategy = payload.strategy ?? "delete";
        if (strategy !== "delete" && strategy !== "keep_latest") {
          throw new ApiError(422, `Invalid thread prune strategy '${strategy}'`);
        }
        const count = await pruner.prune(payload.thread_ids as string[], visible, strategy);
        return { pruned_count: count, deleted: strategy === "delete" ? count : 0,
          pruned: strategy === "keep_latest" ? count : 0 };
      },
    },
    runs: {
      async create(threadId, payload) {
        const strategy = multitaskStrategy(payload.multitask_strategy);
        const assistantId = String(payload.assistant_id);
        const assistant = await getAssistant(assistantId);
        if (!assistant) throw new ApiError(404, `Assistant '${assistantId}' not found`);
        const thread = threadId ? await getThread(threadId) : await store.createThread({
          metadata: { _ephemeral: true, ...(owner() ? { _owner: owner() } : {}) },
        });
        if (!thread) throw new ApiError(404, `Thread '${threadId}' not found`);
        await store.updateThread(thread.id, { metadata: {
          ...thread.metadata, graph_id: assistant.graphId, assistant_id: assistantId,
        } });
        const command = object(payload.command);
        const metadata = publicEntries(payload.metadata);
        // The engine keeps a run's events for the stream modes it was created with; LangGraph defaults to values.
        const modes = payload.stream_mode == null ? ["values"] : streamModes(payload.stream_mode);
        if (modes.length) metadata.__stream_modes = modes;
        const traceContext = runtime.captureTraceContext?.();
        if (traceContext?.traceparent) metadata.__trace_context = traceContext;
        const resumed = payload.command != null || (payload.input && object(payload.input).respond !== undefined);
        const checkpoint = object(payload.checkpoint);
        const checkpointId = typeof checkpoint.checkpoint_id === "string" ? checkpoint.checkpoint_id
          : typeof payload.checkpoint_id === "string" ? payload.checkpoint_id : undefined;
        const assistantConfig = object(assistant.config);
        const requestedConfig = object(payload.config);
        const config = { ...assistantConfig, ...requestedConfig,
          configurable: { ...object(assistantConfig.configurable), ...publicEntries(requestedConfig.configurable) } };
        const runConfig = checkpointId
          ? { ...config, configurable: { ...object(config.configurable), checkpoint_id: checkpointId } }
          : config;
        if (!resumed && payload.input == null && !checkpointId) {
          const latest = await store.getState(thread.id);
          if (!latest) throw new ApiError(422, "Cannot regenerate a thread without state");
          const graph = runtime.getGraph?.(assistant.graphId);
          if (graph && typeof graph === "object" && "getState" in graph && typeof graph.getState === "function") {
            const native = await (graph.getState as (config: JsonRecord) => Promise<JsonRecord>)({ configurable: { thread_id: thread.id } });
            const values = object(native.values);
            const messages = Array.isArray(values.messages) ? values.messages : [];
            const last = messages.at(-1) as { getType?: () => string; type?: string } | undefined;
            if (last?.getType?.() === "ai" || last?.type === "ai") {
              await runtime.updateState(thread.id, { messages: new Overwrite(messages.slice(0, -1)) });
            }
          }
        }
        const run = await (resumed
          ? runtime.resumeRun({ threadId: thread.id,
            resume: command.resume ?? object(payload.input).respond,
            update: command.update === undefined ? undefined : object(command.update),
            goto: typeof command.goto === "string" ||
              (Array.isArray(command.goto) && command.goto.every(item => typeof item === "string"))
              ? command.goto as string | string[] : undefined,
            graphId: assistant.graphId, assistantId, config: runConfig,
            metadata, multitaskStrategy: strategy })
          : runtime.startRun({ threadId: thread.id, graphId: assistant.graphId, assistantId,
            input: payload.input ?? {}, config: runConfig, metadata, multitaskStrategy: strategy }))
          .catch(cause => { throw cause instanceof ThreadBusyError ? new ApiError(409, BUSY_MESSAGE) : cause; });
        return apiRun(run);
      },
      async get(threadId, runId) { const row = await getRun(runId, threadId); return row ? apiRun(row) : null; },
      async list(threadId, query) {
        if (!await getThread(threadId)) return [];
        return (await store.listRuns(threadId, number(query.limit, 10))).map(apiRun);
      },
      async join(threadId, runId, context) {
        while (!context.request.signal.aborted) {
          const run = await getRun(runId, threadId);
          if (!run) throw new ApiError(404, `Run '${runId}' not found`);
          if (["success", "interrupted", "error", "cancelled"].includes(run.status)) {
            if (run.status === "error") throw new ApiError(500, run.error ?? "Run failed");
            return run.output ?? (await runtime.getGraphState?.(run.threadId))?.values
              ?? (await store.getState(run.threadId))?.values ?? {};
          }
          await Bun.sleep(50);
        }
        throw new ApiError(499, "Request aborted while waiting for run");
      },
      async *events(threadId, runId, lastEventId) {
        if (!await getRun(runId, threadId)) throw new ApiError(404, `Run '${runId}' not found`);
        for await (const event of runtime.stream(runId, { after: number(lastEventId, 0) })) {
          yield { id: event.id, event: event.event, data: event.data };
        }
      },
      async cancel(threadId, runId, action) {
        const existing = await getRun(runId, threadId);
        if (!existing || !["pending", "running"].includes(existing.status)) return false;
        if (runtime.cancelRun) {
          await runtime.cancelRun(runId, action);
          return true;
        }
        const cancelled = await store.cancelRun(runId);
        if (cancelled) {
          const state = await store.getState(cancelled.threadId);
          await store.updateThread(cancelled.threadId, {
            status: state?.interrupts.length ? "interrupted" : "idle",
          });
          await store.appendEvent(runId, "end", { status: "cancelled" });
        }
        return true;
      },
      async cancelMany(payload, action) {
        const status = payload.status;
        if (status != null && (payload.thread_id != null || payload.run_ids != null)) {
          throw new ApiError(422, "Specify either status or thread_id with run_ids");
        }
        let targets: RunRecord[];
        if (status != null) {
          if (!["pending", "running", "all"].includes(String(status))) throw new ApiError(422, `Invalid status '${String(status)}'`);
          targets = [];
          for (const run of await store.listActiveRuns()) {
            if (status !== "all" && run.status !== status) continue;
            if (visible(await store.getThread(run.threadId))) targets.push(run);
          }
        } else {
          if (typeof payload.thread_id !== "string" || !Array.isArray(payload.run_ids) ||
            !payload.run_ids.every(id => typeof id === "string")) {
            throw new ApiError(422, "thread_id and run_ids are required unless status is given");
          }
          if (!await getThread(payload.thread_id)) return 0;
          const ids = new Set(payload.run_ids as string[]);
          targets = (await store.listActiveRuns(payload.thread_id)).filter(run => ids.has(run.id));
        }
        // Later runs first, so cancelling a running run does not briefly start one about to be cancelled.
        for (const run of targets.reverse()) {
          if (runtime.cancelRun) await runtime.cancelRun(run.id, action);
          else await store.cancelRun(run.id);
        }
        return targets.length;
      },
    },
    async health() { return { status: "healthy", database: store.dialect,
      queue: process.env.EXECUTION_MODE === "distributed" ? "bullmq" : "inline" }; },
    async info() { return { name: "Valida", version: "0.1.1", status: "running",
      flags: { assistants: true, threads: true, runs: true, store: Boolean(extensions.store),
        crons: Boolean(extensions.crons), v2_event_streaming: true } }; },
  };
}
