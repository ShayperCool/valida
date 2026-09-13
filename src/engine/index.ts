import { Command, type ProtocolEvent } from "@langchain/langgraph";
import { SpanStatusCode } from "@opentelemetry/api";
import type { Telemetry, TraceCarrier } from "../telemetry.ts";
import type { BaseCheckpointSaver } from "@langchain/langgraph-checkpoint";
import { createStore, type CheckpointRecord, type DatabaseConfig, type JsonObject, type RunRecord, type Store } from "../db/index.js";
import { RunQueue, type QueueConfig } from "../queue/index.js";
import { DrizzleCheckpointer } from "./checkpointer.js";
import { V1TraceHandler } from "./v1_callbacks.js";
import { toWire } from "./wire.js";
export { toWire } from "./wire.js";

export type State = JsonObject;
export interface NodeContext {
  threadId: string; runId: string; node: string; config: JsonObject;
  signal: AbortSignal;
  /** Returns the supplied resume value; before that, suspends the current node. */
  interrupt(value: unknown): unknown;
}
export interface NodeCommand { update?: State; goto?: string | string[]; interrupt?: unknown }
export type NodeResult = State | NodeCommand | void;
export type GraphNode = (state: State, context: NodeContext) => NodeResult | Promise<NodeResult>;
export interface GraphDefinition {
  id: string; entrypoint: string; nodes: Record<string, GraphNode>;
  edges?: Record<string, string | string[]>;
  reducers?: Record<string, (previous: unknown, update: unknown) => unknown>;
  recursionLimit?: number;
}
export interface CompiledGraphLike {
  checkpointer?: BaseCheckpointSaver | boolean;
  stream(input: unknown, config: JsonObject): AsyncIterable<unknown> | Promise<AsyncIterable<unknown>>;
  streamEvents?(input: any, options: any): AsyncIterable<ProtocolEvent> | Promise<AsyncIterable<ProtocolEvent>>;
  getState(config: JsonObject): Promise<any>;
  getStateHistory?(config: JsonObject, options?: { limit?: number }): AsyncIterable<any>;
  updateState?(config: JsonObject, values: unknown, asNode?: string): Promise<unknown>;
}
export interface CompiledGraphDefinition { id: string; graph: CompiledGraphLike | ((checkpointer: BaseCheckpointSaver) => CompiledGraphLike) }
export interface RuntimeConfig {
  db: DatabaseConfig; queue?: QueueConfig; inline?: boolean; telemetry?: Telemetry | null;
  runTimeoutMs?: number; runLeaseMs?: number; recoveryPollMs?: number;
}
export interface StartRunOptions {
  threadId: string; graphId: string; assistantId?: string | null;
  input?: unknown; config?: JsonObject; metadata?: JsonObject;
}
export interface ResumeRunOptions {
  threadId: string; resume: unknown; graphId?: string;
  assistantId?: string | null; config?: JsonObject; metadata?: JsonObject;
  update?: JsonObject; goto?: string | string[];
}
export interface StreamOptions { after?: number; pollMs?: number; signal?: AbortSignal }
export interface V2RecordedEvent { id: string; event: ProtocolEvent }
export interface GraphCheckpointConfig extends JsonObject { thread_id: string; checkpoint_id?: string; checkpoint_ns?: string }
export interface GraphStateSnapshot {
  values: State; next: string[]; tasks: unknown[]; interrupts: unknown[];
  metadata: JsonObject; config: GraphCheckpointConfig | null;
  parentConfig: GraphCheckpointConfig | null; createdAt: string | null;
}

class GraphInterrupted extends Error {
  constructor(readonly value: unknown) { super("Graph interrupted"); }
}
const isObject = (value: unknown): value is State => value !== null && typeof value === "object" && !Array.isArray(value);
const stateOf = (value: unknown): State => isObject(value) ? value : value == null ? {} : { input: value };
const ends = (node: string) => node === "END" || node === "__end__";
const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const completed = new Set(["success", "error", "interrupted", "cancelled"]);
type RunControl = {
  controller: AbortController; leaseUntil: string;
  heartbeat?: Promise<void>; heartbeatTimer?: ReturnType<typeof setInterval>;
  timeoutTimer?: ReturnType<typeof setTimeout>;
};

export class GraphRuntime {
  readonly store: Store;
  readonly checkpointer: DrizzleCheckpointer;
  private readonly queue?: RunQueue;
  private readonly inline: boolean;
  private readonly active = new Set<string>();
  private readonly controls = new Map<string, RunControl>();
  private readonly telemetry?: Telemetry | null;
  private recoveryTimer?: ReturnType<typeof setInterval>;
  private workerStarted = false;
  private readonly runTimeoutMs: number;
  private readonly runLeaseMs: number;
  private readonly recoveryPollMs: number;
  private readonly graphs = new Map<string, { kind: "custom"; graph: GraphDefinition } | { kind: "compiled"; graph: CompiledGraphLike }>();

  constructor(store: Store, config: Omit<RuntimeConfig,"db"> = {}) {
    this.store = store;
    this.telemetry = config.telemetry;
    this.checkpointer = new DrizzleCheckpointer(store);
    this.queue = config.queue ? new RunQueue(config.queue) : undefined;
    this.inline = config.inline ?? !config.queue;
    this.runTimeoutMs = config.runTimeoutMs ?? 3_600_000;
    this.runLeaseMs = config.runLeaseMs ?? 60_000;
    this.recoveryPollMs = config.recoveryPollMs ?? 5_000;
    if (!Number.isFinite(this.runTimeoutMs) || this.runTimeoutMs < 0) throw new Error("runTimeoutMs must be non-negative");
    if (!Number.isFinite(this.runLeaseMs) || this.runLeaseMs < 50) throw new Error("runLeaseMs must be at least 50 ms");
    if (!Number.isFinite(this.recoveryPollMs) || this.recoveryPollMs < 10) throw new Error("recoveryPollMs must be at least 10 ms");
    if (this.inline) {
      this.recoveryTimer = setInterval(() => { void this.recoverPendingRuns(); }, this.recoveryPollMs);
      this.recoveryTimer.unref?.();
    }
  }

  registerGraph(definition: GraphDefinition | CompiledGraphDefinition): void {
    if ("nodes" in definition) {
      if (!definition.nodes[definition.entrypoint]) throw new Error(`Graph ${definition.id}: entrypoint ${definition.entrypoint} is missing`);
      this.graphs.set(definition.id, { kind: "custom", graph: definition });
      if (this.inline) queueMicrotask(() => { void this.recoverPendingRuns(); });
      return;
    }
    const graph = typeof definition.graph === "function" ? definition.graph(this.checkpointer) : definition.graph;
    // CompiledStateGraph exposes this property, even when compiled without a saver.
    graph.checkpointer = this.checkpointer;
    this.graphs.set(definition.id, { kind: "compiled", graph });
    if (this.inline) queueMicrotask(() => { void this.recoverPendingRuns(); });
  }

  listGraphs(): string[] { return [...this.graphs.keys()]; }
  hasGraph(id: string): boolean { return this.graphs.has(id); }
  supportsV2(id: string): boolean {
    const entry = this.graphs.get(id);
    return entry?.kind === "compiled" && typeof entry.graph.streamEvents === "function";
  }
  getGraph(id: string): GraphDefinition | CompiledGraphLike | null { return this.graphs.get(id)?.graph ?? null; }
  captureTraceContext(): TraceCarrier | undefined { return this.telemetry?.injectTraceContext(); }

  createThread(value: { id?: string; metadata?: JsonObject } = {}) { return this.store.createThread(value); }
  getThread(id: string) { return this.store.getThread(id); }
  getState(threadId: string) { return this.store.getState(threadId); }
  getHistory(threadId: string, limit?: number) { return this.store.getHistory(threadId, limit); }
  getRun(runId: string) { return this.store.getRun(runId); }
  getRuns(threadId: string, limit?: number) { return this.store.listRuns(threadId, limit); }

  private snapshot(raw: Record<string, any>): GraphStateSnapshot {
    const config = raw.config?.configurable;
    const parent = raw.parentConfig?.configurable;
    const tasks = Array.isArray(raw.tasks) ? toWire(raw.tasks) as unknown[] : [];
    return {
      values: stateOf(toWire(raw.values)),
      next: Array.isArray(raw.next) ? raw.next.map(String) : [],
      tasks,
      interrupts: tasks.flatMap(task => isObject(task) && Array.isArray(task.interrupts) ? task.interrupts : []),
      metadata: stateOf(toWire(raw.metadata)),
      config: config?.thread_id ? { thread_id: String(config.thread_id),
        checkpoint_id: config.checkpoint_id ? String(config.checkpoint_id) : undefined,
        checkpoint_ns: String(config.checkpoint_ns ?? "") } : null,
      parentConfig: parent?.thread_id ? { thread_id: String(parent.thread_id),
        checkpoint_id: parent.checkpoint_id ? String(parent.checkpoint_id) : undefined,
        checkpoint_ns: String(parent.checkpoint_ns ?? "") } : null,
      createdAt: typeof raw.createdAt === "string" ? raw.createdAt : null,
    };
  }

  /** Native LangGraph state with the checkpoint IDs needed for Edit/Refresh branching. */
  async getGraphState(threadId: string, checkpointId?: string, graphId?: string): Promise<GraphStateSnapshot | null> {
    const latest = await this.store.getState(threadId);
    const registered = this.graphs.get(graphId ?? latest?.graphId ?? "");
    if (registered?.kind === "compiled") {
      const raw = await registered.graph.getState({ configurable: { thread_id: threadId,
        ...(checkpointId ? { checkpoint_id: checkpointId } : {}) } });
      return raw?.config?.configurable?.checkpoint_id ? this.snapshot(raw) : null;
    }
    const record = checkpointId ? await this.store.getCheckpoint(checkpointId) : latest;
    if (!record || record.threadId !== threadId) return null;
    return { values: record.values, next: record.next, tasks: record.tasks, interrupts: record.interrupts,
      metadata: { step: record.step, source: "loop" },
      config: { thread_id: threadId, checkpoint_id: record.id, checkpoint_ns: "" },
      parentConfig: record.parentId ? { thread_id: threadId, checkpoint_id: record.parentId, checkpoint_ns: "" } : null,
      createdAt: record.createdAt };
  }

  /** Return every native graph step for compiled graphs, including the initial checkpoint. */
  async getGraphHistory(threadId: string, limit = 100, graphId?: string): Promise<GraphStateSnapshot[]> {
    const latest = await this.store.getState(threadId);
    const registered = this.graphs.get(graphId ?? latest?.graphId ?? "");
    if (registered?.kind === "compiled" && registered.graph.getStateHistory) {
      const history: GraphStateSnapshot[] = [];
      for await (const raw of registered.graph.getStateHistory({ configurable: { thread_id: threadId } }, { limit })) {
        history.push(this.snapshot(raw));
      }
      return history;
    }
    return (await this.store.getHistory(threadId, limit)).map(record => ({
      values: record.values, next: record.next, tasks: record.tasks, interrupts: record.interrupts,
      metadata: { step: record.step, source: "loop" },
      config: { thread_id: threadId, checkpoint_id: record.id, checkpoint_ns: "" },
      parentConfig: record.parentId ? { thread_id: threadId, checkpoint_id: record.parentId, checkpoint_ns: "" } : null,
      createdAt: record.createdAt,
    }));
  }

  async startRun(options: StartRunOptions): Promise<RunRecord> {
    if (!this.graphs.has(options.graphId)) throw new Error(`Unknown graph: ${options.graphId}`);
    if (!await this.store.getThread(options.threadId)) await this.store.createThread({ id: options.threadId });
    const previous = await this.store.getState(options.threadId);
    const branchId = isObject(options.config?.configurable) ? options.config.configurable.checkpoint_id : undefined;
    if (previous?.interrupts.length && !branchId) throw new Error(`Thread ${options.threadId} is interrupted; call resumeRun`);
    const allowed = branchId ? ["idle", "error", "interrupted"] as const : ["idle", "error"] as const;
    if (!await this.store.claimThread(options.threadId, [...allowed])) throw new Error(`Thread ${options.threadId} is busy`);
    try {
      const run = await this.store.createRun(options);
      await this.store.appendEvent(run.id, "metadata", { run_id: run.id, thread_id: run.threadId });
      await this.dispatch(run.id);
      return run;
    } catch (error) {
      await this.store.updateThread(options.threadId, { status: "error" });
      throw error;
    }
  }

  async resumeRun(options: ResumeRunOptions): Promise<RunRecord> {
    const latest = await this.store.getState(options.threadId);
    if (!latest || !latest.interrupts.length) throw new Error(`Thread ${options.threadId} has no pending interrupt`);
    const graphId = options.graphId ?? latest.graphId;
    if (graphId !== latest.graphId) throw new Error(`Cannot resume ${latest.graphId} with graph ${graphId}`);
    if (!await this.store.claimThread(options.threadId, ["interrupted"])) throw new Error(`Thread ${options.threadId} is busy`);
    try {
    const run = await this.store.createRun({
      threadId: options.threadId, graphId, assistantId: options.assistantId,
      input: null, resume: options.resume, config: options.config,
      metadata: { ...options.metadata, __resumeProvided: true,
        __commandUpdate: options.update, __commandGoto: options.goto },
    });
    await this.store.appendEvent(run.id, "metadata", { run_id: run.id, thread_id: run.threadId });
    await this.dispatch(run.id);
    return run;
    } catch (error) {
      await this.store.updateThread(options.threadId, { status: "error" });
      throw error;
    }
  }

  private async dispatch(runId: string): Promise<void> {
    if (this.queue) {
      try { await this.queue.enqueue(runId); }
      catch (error) {
        // The durable database row is the fallback queue. A distributed worker polls it.
        console.warn(`Redis enqueue failed for run ${runId}; database recovery will process it: ${String(error)}`);
      }
    }
    if (this.inline) queueMicrotask(() => { void this.executeRun(runId).catch(() => {}); });
  }

  startWorker(): void {
    if (!this.queue) throw new Error("startWorker requires queue.redisUrl");
    const startQueue = () => { void this.queue!.start(runId => this.executeRun(runId)).catch(() => {}); };
    startQueue();
    this.workerStarted = true;
    this.recoveryTimer ??= setInterval(() => {
      startQueue();
      void this.recoverPendingRuns();
    }, this.recoveryPollMs);
    queueMicrotask(() => { void this.recoverPendingRuns(); });
  }

  async recoverPendingRuns(): Promise<void> {
    if (!this.inline && !this.workerStarted) return;
    for (const run of await this.store.listRunnableRuns()) {
      if (this.graphs.has(run.graphId) && !this.active.has(run.id)) {
        void this.executeRun(run.id).catch(() => {});
      }
    }
  }

  async executeRun(runId: string): Promise<void> {
    if (this.active.has(runId)) return;
    this.active.add(runId);
    try {
      let recovering = false;
      for (;;) {
        const before = await this.store.getRun(runId);
        if (!before || completed.has(before.status)) return;
        recovering = before.status === "running";
        if (await this.store.claimRun(runId, this.runLeaseMs)) break;
        // A stalled BullMQ job may be retried before its database lease expires.
        // Keep the replacement job alive until the owner finishes or the lease expires.
        const remaining = before.leaseUntil ? Date.parse(before.leaseUntil) - Date.now() : 0;
        await sleep(Math.min(Math.max(remaining + 20, 100), 5_000));
      }
      const run = await this.store.getRun(runId);
      if (!run) return;
      const control: RunControl = { controller: new AbortController(), leaseUntil: run.leaseUntil! };
      this.controls.set(runId, control);
      const renew = () => {
        if (control.heartbeat || control.controller.signal.aborted) return;
        control.heartbeat = (async () => {
          try {
            const next = await this.store.renewRun(run.id, control.leaseUntil, this.runLeaseMs);
            if (next) control.leaseUntil = next;
            else control.controller.abort(new Error("Run cancelled or lease lost"));
          } catch (error) {
            control.controller.abort(error);
          }
        })().finally(() => { control.heartbeat = undefined; });
      };
      control.heartbeatTimer = setInterval(renew, Math.max(10, Math.floor(this.runLeaseMs / 3)));
      if (this.runTimeoutMs) {
        control.timeoutTimer = setTimeout(() => {
          control.controller.abort(new Error(`Run timed out after ${this.runTimeoutMs} ms`));
        }, this.runTimeoutMs);
      }
      const registered = this.graphs.get(run.graphId);
      if (!registered) {
        await this.fail(run, new Error(`Graph ${run.graphId} is not registered in this worker`));
        return;
      }
      const execute = async () => {
        await this.store.appendEvent(run.id, "run", { status: "running", recovering });
        try {
          const interrupted = new Promise<never>((_, reject) => {
            control.controller.signal.addEventListener("abort", () => reject(control.controller.signal.reason), { once: true });
          });
          await Promise.race([
            registered.kind === "custom"
              ? this.executeCustom(run, registered.graph, recovering)
              : this.executeCompiled(run, registered.graph, recovering),
            interrupted,
          ]);
        } catch (error) { await this.fail(run, error); }
      };
      if (this.telemetry) {
        const carrier = run.metadata.__trace_context;
        await this.telemetry.withRunSpan({ graphId: run.graphId, runId: run.id,
          threadId: run.threadId, assistantId: run.assistantId,
          traceContext: isObject(carrier) ? carrier as TraceCarrier : undefined,
        }, async span => {
          await execute();
          const finished = await this.store.getRun(run.id);
          if (finished) {
            span.setAttribute("valida.run.status", finished.status);
            if (finished.status === "error") {
              span.setStatus({ code: SpanStatusCode.ERROR, message: finished.error ?? undefined });
            }
          }
        });
      } else {
        await execute();
      }
    } finally {
      const control = this.controls.get(runId);
      if (control?.heartbeatTimer) clearInterval(control.heartbeatTimer);
      if (control?.timeoutTimer) clearTimeout(control.timeoutTimer);
      await control?.heartbeat;
      this.controls.delete(runId);
      this.active.delete(runId);
    }
  }

  private async fail(run: RunRecord, error: unknown): Promise<void> {
    const message = error instanceof Error ? error.message : String(error);
    if (!await this.finishRun(run, "error", undefined, message)) return;
    await this.store.updateThread(run.threadId, { status: "error" });
    await this.store.appendEvent(run.id, "error", { message });
    await this.store.appendEvent(run.id, "end", { status: "error" });
  }

  private async ensureActive(runId: string): Promise<void> {
    const control = this.controls.get(runId);
    if (!control) throw new Error("Run execution has ended");
    if (control.controller.signal.aborted) throw control.controller.signal.reason;
    let current = await this.store.getRun(runId);
    // A heartbeat may have renewed between reading the row and reading the local token.
    if (current?.status === "running" && current.leaseUntil !== control.leaseUntil) {
      current = await this.store.getRun(runId);
    }
    if (current?.status !== "running" || current.leaseUntil !== control.leaseUntil) {
      control.controller.abort(new Error("Run cancelled or lease lost"));
      throw control.controller.signal.reason;
    }
  }

  private async finishRun(run: RunRecord, status: "success" | "interrupted" | "error",
    output?: unknown, error?: string): Promise<boolean> {
    const control = this.controls.get(run.id);
    if (!control) return false;
    if (control.heartbeatTimer) clearInterval(control.heartbeatTimer);
    await control.heartbeat;
    if (status !== "error" && control.controller.signal.aborted) return false;
    const finished = await this.store.finishRun(run.id, control.leaseUntil, { status, output, error });
    if (finished && control.timeoutTimer) clearTimeout(control.timeoutTimer);
    return Boolean(finished);
  }

  private merge(base: State, update: State, graph: GraphDefinition): State {
    const next = { ...base };
    for (const [key, value] of Object.entries(update)) {
      next[key] = graph.reducers?.[key] ? graph.reducers[key]!(base[key], value) : value;
    }
    return next;
  }

  private async executeCustom(run: RunRecord, graph: GraphDefinition, recovering: boolean): Promise<void> {
    await this.ensureActive(run.id);
    const previous = await this.store.getState(run.threadId);
    const resuming = run.metadata.__resumeProvided === true;
    const continuing = recovering && previous?.runId === run.id;
    let values = previous?.values ?? {};
    let next = resuming || continuing ? previous?.next ?? [] : [graph.entrypoint];
    let step = previous?.step ?? -1;
    let parentId = previous?.id ?? null;
    if (!resuming && !continuing) values = this.merge(values, stateOf(run.input), graph);
    if (resuming && !continuing &&
      (isObject(run.metadata.__commandUpdate) || run.metadata.__commandGoto !== undefined)) {
      if (isObject(run.metadata.__commandUpdate)) {
        values = this.merge(values, run.metadata.__commandUpdate, graph);
      }
      const goto = run.metadata.__commandGoto;
      if (typeof goto === "string" || (Array.isArray(goto) && goto.every(item => typeof item === "string"))) {
        next = Array.isArray(goto) ? [...goto, ...next.slice(1)] : [goto, ...next.slice(1)];
      }
      step++;
      await this.ensureActive(run.id);
      const updated = await this.store.createCheckpoint({ threadId: run.threadId, runId: run.id,
        graphId: graph.id, step, values, next, tasks: [], interrupts: [], parentId });
      parentId = updated.id;
      await this.store.appendEvent(run.id, "values", values);
    }
    if (!previous || (!resuming && !continuing)) {
      step++;
      await this.ensureActive(run.id);
      const initial = await this.store.createCheckpoint({ threadId: run.threadId, runId: run.id, graphId: graph.id,
        step, values, next, tasks: [], interrupts: [], parentId });
      parentId = initial.id;
      await this.store.appendEvent(run.id, "values", values);
    }
    const limit = graph.recursionLimit ?? 100;
    let localSteps = 0;
    while (next.length) {
      await this.ensureActive(run.id);
      if (++localSteps > limit) throw new Error(`Graph ${graph.id} exceeded recursion limit ${limit}`);
      const current = next.shift()!;
      if (ends(current)) continue;
      const node = graph.nodes[current];
      if (!node) throw new Error(`Graph ${graph.id}: node ${current} is missing`);
      const context: NodeContext = {
        threadId: run.threadId, runId: run.id, node: current, config: run.config,
        signal: this.controls.get(run.id)!.controller.signal,
        interrupt: value => { if (resuming) return run.resume; throw new GraphInterrupted(value); },
      };
      let result: NodeResult;
      try { result = await node(values, context); }
      catch (error) {
        if (!(error instanceof GraphInterrupted)) throw error;
        await this.interruptCustom(run, graph, values, [current, ...next], step + 1, parentId, error.value);
        return;
      }
      await this.ensureActive(run.id);
      const command = isObject(result) && ("goto" in result || "interrupt" in result || "update" in result)
        ? result as NodeCommand : { update: result as State | undefined };
      if (Object.prototype.hasOwnProperty.call(command, "interrupt")) {
        await this.interruptCustom(run, graph, values, [current, ...next], step + 1, parentId, command.interrupt);
        return;
      }
      const update = command.update ?? {};
      values = this.merge(values, update, graph);
      const destination = command.goto ?? graph.edges?.[current] ?? "__end__";
      next = [...(Array.isArray(destination) ? destination : [destination]), ...next].filter(n => !ends(n));
      step++;
      await this.ensureActive(run.id);
      const saved = await this.store.createCheckpoint({ threadId: run.threadId, runId: run.id, graphId: graph.id,
        step, values, next, tasks: [], interrupts: [], parentId });
      parentId = saved.id;
      await this.store.appendEvent(run.id, "updates", { [current]: update });
      await this.store.appendEvent(run.id, "values", values);
    }
    if (!await this.finishRun(run, "success", values)) return;
    await this.store.updateThread(run.threadId, { status: "idle" });
    await this.store.appendEvent(run.id, "end", { status: "success", output: values });
  }

  private async interruptCustom(run: RunRecord, graph: GraphDefinition, values: State, next: string[], step: number, parentId: string | null, value: unknown): Promise<void> {
    await this.ensureActive(run.id);
    await this.store.createCheckpoint({ threadId: run.threadId, runId: run.id, graphId: graph.id,
      step, values, next, tasks: [{ name: next[0] }], interrupts: [{ value }], parentId });
    if (!await this.finishRun(run, "interrupted", values)) return;
    await this.store.updateThread(run.threadId, { status: "interrupted" });
    await this.store.appendEvent(run.id, "updates", { __interrupt__: [{ value }] });
    await this.store.appendEvent(run.id, "end", { status: "interrupted", output: values });
  }

  private async executeCompiled(run: RunRecord, graph: CompiledGraphLike, recovering: boolean): Promise<void> {
    await this.ensureActive(run.id);
    const configurable = { ...(isObject(run.config.configurable) ? run.config.configurable : {}), thread_id: run.threadId };
    const config = { ...run.config, configurable, streamMode: ["updates", "values"],
      signal: this.controls.get(run.id)!.controller.signal };
    const native = recovering ? await this.checkpointer.getTuple({ configurable: { thread_id: run.threadId } }) : undefined;
    const resumeNative = native && native.checkpoint.ts >= run.createdAt;
    const input = run.metadata.__resumeProvided === true
      ? new Command({ resume: run.resume, update: run.metadata.__commandUpdate as JsonObject | undefined,
        goto: run.metadata.__commandGoto as string | string[] | undefined })
      : resumeNative ? null : run.input;
    let terminalV2: ProtocolEvent | null = null;
    let lastV2Seq = -1;
    let sawInterruptUpdate = false;
    const requestedIds = new Set<string>();
    // Native stream callbacks and the protocol iterator can emit concurrently.
    // Serialize writes so each event gets a unique, replayable database sequence.
    let writeTail: Promise<unknown> = Promise.resolve();
    const append = (name: string, data: unknown) => {
      const pending = writeTail.then(() => this.store.appendEvent(run.id, name, data));
      writeTail = pending.catch(() => undefined);
      return pending;
    };
    if (graph.streamEvents) {
      const trace = new V1TraceHandler(event => append("trace", event));
      const native = await graph.streamEvents(input, { ...config, callbacks: [trace], version: "v3" });
      for await (const event of native) {
        await this.ensureActive(run.id);
        const wire = toWire(event) as ProtocolEvent;
        lastV2Seq = Math.max(lastV2Seq, wire.seq);
        if (wire.method === "input.requested") {
          const id = isObject(wire.params.data) ? wire.params.data.interrupt_id : undefined;
          if (typeof id === "string") requestedIds.add(id);
        }
        if (wire.method === "lifecycle" && wire.params.namespace.length === 0 &&
          isObject(wire.params.data) && wire.params.data.event === "completed") {
          terminalV2 = wire;
          continue;
        }
        await append("v2", wire);
        const data = wire.params.data;
        if (wire.params.namespace.length === 0 && wire.method === "updates") {
          const update = isObject(data) && typeof data.node === "string"
            ? { [data.node]: data.values } : data;
          if (isObject(update) && "__interrupt__" in update) sawInterruptUpdate = true;
          await append("updates", update);
        } else if (wire.params.namespace.length === 0 && wire.method === "values") {
          await append("values", data);
        } else if (["custom", "tools", "checkpoints", "tasks"].includes(wire.method)) {
          await append(wire.method, data);
        }
      }
    } else {
      for await (const chunk of await graph.stream(input, config)) {
        await this.ensureActive(run.id);
        const [mode, data] = Array.isArray(chunk) && typeof chunk[0] === "string" ? chunk as [string, unknown] : ["updates", chunk];
        await append(mode, toWire(data));
      }
    }
    await writeTail;
    await this.ensureActive(run.id);
    const latestConfigurable: JsonObject = { ...configurable };
    delete latestConfigurable.checkpoint_id;
    const snapshot = await graph.getState({ ...config, configurable: latestConfigurable });
    const values = stateOf(toWire(snapshot.values));
    const next = Array.isArray(snapshot.next) ? snapshot.next.map(String) : [];
    const tasks = Array.isArray(snapshot.tasks) ? toWire(snapshot.tasks) as unknown[] : [];
    const interrupts = tasks.flatMap((task: unknown) => isObject(task) && Array.isArray(task.interrupts) ? task.interrupts : []);
    const previous = await this.store.getState(run.threadId);
    await this.ensureActive(run.id);
    await this.store.createCheckpoint({ threadId: run.threadId, runId: run.id, graphId: run.graphId,
      step: (previous?.step ?? 0) + 1, values, next, tasks, interrupts,
      parentId: previous?.id ?? null });
    const status = interrupts.length || next.length ? "interrupted" : "success";
    if (interrupts.length && !sawInterruptUpdate) await append("updates", { __interrupt__: interrupts });
    if (graph.streamEvents) {
      if (status === "interrupted") {
        let seq = terminalV2?.seq ?? lastV2Seq + 1;
        for (const entry of interrupts) {
          const id = isObject(entry) && typeof entry.id === "string" ? entry.id : `${run.id}:${seq}`;
          if (requestedIds.has(id)) continue;
          const request: ProtocolEvent = { type: "event", seq: seq++, method: "input.requested",
            params: { namespace: [], timestamp: Date.now(), data: {
              interrupt_id: id, payload: isObject(entry) ? entry.value : entry,
            } } };
          await append("v2", request);
        }
        await append("v2", { type: "event", seq, method: "lifecycle",
          params: { namespace: [], timestamp: Date.now(), data: { event: "interrupted", graph_name: "root" } } });
      } else if (terminalV2) {
        await append("v2", terminalV2);
      }
    }
    if (!await this.finishRun(run, status, values)) return;
    await this.store.updateThread(run.threadId, { status: status === "success" ? "idle" : "interrupted" });
    await append("end", { status, output: values });
  }

  async updateState(threadId: string, update: State, asNode?: string): Promise<CheckpointRecord> {
    const previous = await this.store.getState(threadId);
    if (!previous) throw new Error(`Thread ${threadId} has no state`);
    const registered = this.graphs.get(previous.graphId);
    if (registered?.kind === "compiled") {
      await registered.graph.updateState?.({ configurable: { thread_id: threadId } }, update, asNode);
      const snapshot = await registered.graph.getState({ configurable: { thread_id: threadId } });
      return this.store.createCheckpoint({ threadId, runId: previous.runId, graphId: previous.graphId,
        step: previous.step + 1, values: stateOf(toWire(snapshot.values)),
        next: Array.isArray(snapshot.next) ? snapshot.next.map(String) : [],
        tasks: Array.isArray(snapshot.tasks) ? snapshot.tasks : [], interrupts: [], parentId: previous.id });
    }
    const values = registered?.kind === "custom" ? this.merge(previous.values, update, registered.graph) : { ...previous.values, ...update };
    return this.store.createCheckpoint({ threadId, runId: previous.runId, graphId: previous.graphId,
      step: previous.step + 1, values, next: previous.next, tasks: previous.tasks,
      interrupts: previous.interrupts, parentId: previous.id });
  }

  async cancelRun(runId: string): Promise<RunRecord | null> { return this.store.cancelRun(runId); }
  async waitRun(runId: string, pollMs = 50): Promise<RunRecord> {
    for (;;) {
      const run = await this.store.getRun(runId);
      if (!run) throw new Error(`Run ${runId} was deleted`);
      if (completed.has(run.status)) return run;
      await sleep(pollMs);
    }
  }
  async *stream(runId: string, options: StreamOptions = {}): AsyncGenerator<{ event: string; data: unknown; id: string }> {
    let after = options.after ?? 0;
    for (;;) {
      if (options.signal?.aborted) return;
      const batch = await this.store.listEvents(runId, after);
      for (const item of batch) {
        after = item.seq;
        if (item.event !== "v2" && item.event !== "trace") {
          yield { event: item.event, data: item.data, id: String(item.seq) };
        }
      }
      const run = await this.store.getRun(runId);
      if (!run || (completed.has(run.status) && batch.length === 0)) return;
      await sleep(options.pollMs ?? 100);
    }
  }
  /** Replay and follow every stored event, including v3 envelopes and captured v1 callbacks. */
  async *streamAll(runId: string, options: StreamOptions = {}): AsyncGenerator<{ event: string; data: unknown; id: string }> {
    let after = options.after ?? 0;
    for (;;) {
      if (options.signal?.aborted) return;
      const batch = await this.store.listEvents(runId, after);
      for (const item of batch) {
        after = item.seq;
        yield { event: item.event, data: item.data, id: String(item.seq) };
      }
      const run = await this.store.getRun(runId);
      if (!run || (completed.has(run.status) && batch.length === 0)) return;
      await sleep(options.pollMs ?? 100);
    }
  }
  /** Replay and follow native LangGraph v3 protocol envelopes for one run. */
  async *streamV2(runId: string, options: StreamOptions = {}): AsyncGenerator<V2RecordedEvent> {
    let after = options.after ?? 0;
    for (;;) {
      if (options.signal?.aborted) return;
      const batch = await this.store.listEvents(runId, after);
      for (const item of batch) {
        after = item.seq;
        if (item.event === "v2") yield { id: String(item.seq), event: item.data as ProtocolEvent };
      }
      const run = await this.store.getRun(runId);
      if (!run || (completed.has(run.status) && batch.length === 0)) return;
      await sleep(options.pollMs ?? 100);
    }
  }
  async close(): Promise<void> { if (this.recoveryTimer) clearInterval(this.recoveryTimer); await this.queue?.close(); await this.store.close(); }
}

export async function createRuntime(config: RuntimeConfig): Promise<GraphRuntime> {
  return new GraphRuntime(await createStore(config.db), config);
}
