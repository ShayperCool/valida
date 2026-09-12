import type { Store, AssistantRecord, ThreadRecord, RunRecord, CheckpointRecord } from "./db/index.ts";
import type { PlatformAdapter, JsonRecord, Assistant, Thread, Run, ThreadState, Checkpoint } from "./api/types.ts";
import { ApiError } from "./api/types.ts";
import { currentUser } from "./auth.ts";

type RuntimeHandle = {
  startRun(input: {
    threadId: string; graphId: string; assistantId?: string;
    input?: unknown; config?: JsonRecord; metadata?: JsonRecord;
  }): Promise<RunRecord>;
  resumeRun(input: { threadId: string; resume: unknown; graphId?: string }): Promise<RunRecord>;
  getRun(id: string): Promise<RunRecord | null>;
  stream(id: string, options?: { after?: number; pollMs?: number }): AsyncIterable<{ event: string; data: unknown; id: string }>;
  updateState(threadId: string, update: JsonRecord, asNode?: string): Promise<CheckpointRecord>;
};

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
  status: record.status === "cancelled" ? "error" : record.status,
  metadata: record.metadata, created_at: record.createdAt, updated_at: record.updatedAt,
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

function owner(): string | undefined { return currentUser.getStore()?.identity; }
function visible(record: ThreadRecord | null): boolean {
  const identity = owner();
  return !record || !identity || !record.metadata._owner || record.metadata._owner === identity;
}
function ensureVisible(record: ThreadRecord | null): ThreadRecord | null {
  if (!visible(record)) throw new ApiError(403, "Thread access denied");
  return record;
}
function matchesMetadata(value: JsonRecord, filter: JsonRecord): boolean {
  return Object.entries(filter).every(([key, expected]) => value[key] === expected);
}

export async function seedDefaultAssistants(store: Store, graphIds: string[]): Promise<void> {
  for (const id of graphIds) {
    if (!await store.getAssistant(id)) await store.createAssistant({ id, graphId: id, name: id });
  }
}

export function createPlatformAdapter(runtime: RuntimeHandle, store: Store, graphIds: string[]): PlatformAdapter {
  async function getThread(id: string) { return ensureVisible(await store.getThread(id)); }
  async function threadWithValues(row: ThreadRecord): Promise<Thread> {
    return { ...apiThread(row), values: (await store.getState(row.id))?.values ?? {} };
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

  return {
    assistants: {
      async create(payload) {
        const graphId = String(payload.graph_id);
        if (!graphIds.includes(graphId)) throw new ApiError(422, `Unknown graph: ${graphId}`);
        const created = await store.createAssistant({
          id: typeof payload.assistant_id === "string" ? payload.assistant_id : undefined,
          graphId, name: typeof payload.name === "string" ? payload.name : graphId,
          description: typeof payload.description === "string" ? payload.description : null,
          config: object(payload.config), metadata: object(payload.metadata),
        });
        return apiAssistant(created);
      },
      async search(query) {
        const rows = await store.listAssistants(100_000);
        const filtered = rows.filter(row =>
          (!query.graph_id || row.graphId === query.graph_id) &&
          (!query.name || row.name === query.name) &&
          matchesMetadata(row.metadata, object(query.metadata)),
        );
        return filtered.slice(number(query.offset, 0), number(query.offset, 0) + number(query.limit, 10)).map(apiAssistant);
      },
      async get(id) { const row = await getAssistant(id); return row ? apiAssistant(row) : null; },
      async update(id, payload) {
        const row = await store.updateAssistant(id, {
          name: typeof payload.name === "string" ? payload.name : undefined,
          description: typeof payload.description === "string" ? payload.description : undefined,
          config: payload.config === undefined ? undefined : object(payload.config),
          metadata: payload.metadata === undefined ? undefined : object(payload.metadata),
        });
        return row ? apiAssistant(row) : null;
      },
      async delete(id) {
        if (!await store.getAssistant(id)) return false;
        await store.deleteAssistant(id);
        return true;
      },
      async graph(id) {
        const assistant = await getAssistant(id);
        return assistant ? { nodes: [{ id: "__start__" }, { id: assistant.graphId }, { id: "__end__" }],
          edges: [{ source: "__start__", target: assistant.graphId }, { source: assistant.graphId, target: "__end__" }] } : null;
      },
      async schemas(id) {
        return await getAssistant(id) ? {
          graph_id: id, input_schema: { type: "object", additionalProperties: true },
          output_schema: { type: "object", additionalProperties: true },
          state_schema: { type: "object", additionalProperties: true },
          config_schema: { type: "object", additionalProperties: true },
        } : null;
      },
      async subgraphs(id) { return await getAssistant(id) ? {} : null; },
    },
    threads: {
      async create(payload) {
        const metadata = object(payload.metadata);
        if (owner()) metadata._owner = owner();
        const row = await store.createThread({
          id: typeof payload.thread_id === "string" ? payload.thread_id : undefined,
          metadata,
        });
        return threadWithValues(row);
      },
      async search(query) {
        const rows = await store.listThreads(100_000);
        const filtered = rows.filter(row => visible(row) &&
          (!query.status || row.status === query.status) &&
          matchesMetadata(row.metadata, object(query.metadata)));
        return Promise.all(filtered.slice(number(query.offset, 0), number(query.offset, 0) + number(query.limit, 10)).map(threadWithValues));
      },
      async get(id) { const row = await getThread(id); return row ? threadWithValues(row) : null; },
      async update(id, payload) {
        const row = await getThread(id);
        if (!row) return null;
        const updated = await store.updateThread(id, { metadata: { ...row.metadata, ...object(payload.metadata) } });
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
        const row = checkpointId ? await store.getCheckpoint(checkpointId) : await store.getState(id);
        return row && row.threadId !== id ? null : apiState(row, id);
      },
      async updateState(id, payload) {
        if (!await getThread(id)) return null;
        const previous = await store.getState(id);
        const row = previous
          ? await runtime.updateState(id, object(payload.values),
            typeof payload.as_node === "string" ? payload.as_node : undefined)
          : await store.createCheckpoint({
            threadId: id, runId: crypto.randomUUID(), graphId: graphIds[0] ?? "unknown",
            step: 0, values: object(payload.values), next: [], tasks: [], interrupts: [], parentId: null,
          });
        return { thread_id: id, checkpoint_id: row.id, checkpoint_ns: "" };
      },
      async history(id, query) {
        if (!await getThread(id)) return null;
        return (await store.getHistory(id, number(query.limit, 10))).map(row => apiState(row, id));
      },
      async copy(id) {
        const original = await getThread(id);
        if (!original) return null;
        const created = await store.createThread({ metadata: original.metadata });
        const state = await store.getState(id);
        if (state) await store.createCheckpoint({ ...state, id: undefined, threadId: created.id, parentId: null });
        return threadWithValues(created);
      },
    },
    runs: {
      async create(threadId, payload) {
        const assistantId = String(payload.assistant_id);
        const assistant = await getAssistant(assistantId);
        if (!assistant) throw new ApiError(404, `Assistant '${assistantId}' not found`);
        const thread = threadId ? await getThread(threadId) : await store.createThread({ metadata: { _ephemeral: true } });
        if (!thread) throw new ApiError(404, `Thread '${threadId}' not found`);
        await store.updateThread(thread.id, { metadata: {
          ...thread.metadata, graph_id: assistant.graphId, assistant_id: assistantId,
        } });
        const command = object(payload.command);
        const resumed = payload.command != null || (payload.input && object(payload.input).respond !== undefined);
        const run = resumed
          ? await runtime.resumeRun({ threadId: thread.id, resume: command.resume ?? object(payload.input).respond, graphId: assistant.graphId })
          : await runtime.startRun({ threadId: thread.id, graphId: assistant.graphId, assistantId,
            input: payload.input, config: object(payload.config), metadata: object(payload.metadata) });
        return apiRun(run);
      },
      async get(threadId, runId) { const row = await getRun(runId, threadId); return row ? apiRun(row) : null; },
      async list(threadId, query) {
        if (!await getThread(threadId)) return [];
        return (await store.listRuns(threadId, number(query.limit, 10))).map(apiRun);
      },
      async join(threadId, runId) {
        for (let attempt = 0; attempt < 600; attempt++) {
          const run = await getRun(runId, threadId);
          if (!run) throw new ApiError(404, `Run '${runId}' not found`);
          if (["success", "interrupted", "error", "cancelled"].includes(run.status)) {
            if (run.status === "error") throw new ApiError(500, run.error ?? "Run failed");
            return run.output ?? (await store.getState(run.threadId))?.values ?? {};
          }
          await Bun.sleep(50);
        }
        throw new ApiError(504, "Run timed out");
      },
      async *events(threadId, runId, lastEventId) {
        if (!await getRun(runId, threadId)) throw new ApiError(404, `Run '${runId}' not found`);
        for await (const event of runtime.stream(runId, { after: number(lastEventId, 0) })) {
          yield { id: event.id, event: event.event, data: event.data };
        }
      },
      async cancel(threadId, runId) {
        if (!await getRun(runId, threadId)) return false;
        await store.cancelRun(runId);
        return true;
      },
    },
    async health() { return { status: "healthy", database: store.dialect, queue: "ready" }; },
    async info() { return { name: "Valida", version: "0.1.0", status: "running",
      flags: { assistants: true, threads: true, runs: true, v2_event_streaming: false } }; },
  };
}
