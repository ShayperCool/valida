import { Command } from "@langchain/langgraph";
import type { BaseCheckpointSaver } from "@langchain/langgraph-checkpoint";
import { createStore, type CheckpointRecord, type DatabaseConfig, type JsonObject, type RunRecord, type Store } from "../db/index.js";
import { RunQueue, type QueueConfig } from "../queue/index.js";
import { DrizzleCheckpointer } from "./checkpointer.js";
import { toWire } from "./wire.js";

export type State = JsonObject;
export interface NodeContext {
  threadId: string; runId: string; node: string; config: JsonObject;
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
  getState(config: JsonObject): Promise<any>;
  updateState?(config: JsonObject, values: unknown, asNode?: string): Promise<unknown>;
}
export interface CompiledGraphDefinition { id: string; graph: CompiledGraphLike | ((checkpointer: BaseCheckpointSaver) => CompiledGraphLike) }
export interface RuntimeConfig { db: DatabaseConfig; queue?: QueueConfig; inline?: boolean }
export interface StartRunOptions {
  threadId: string; graphId: string; assistantId?: string | null;
  input?: unknown; config?: JsonObject; metadata?: JsonObject;
}
export interface ResumeRunOptions {
  threadId: string; resume: unknown; graphId?: string;
  assistantId?: string | null; config?: JsonObject; metadata?: JsonObject;
}
export interface StreamOptions { after?: number; pollMs?: number; signal?: AbortSignal }

class GraphInterrupted extends Error {
  constructor(readonly value: unknown) { super("Graph interrupted"); }
}
const isObject = (value: unknown): value is State => value !== null && typeof value === "object" && !Array.isArray(value);
const stateOf = (value: unknown): State => isObject(value) ? value : value == null ? {} : { input: value };
const ends = (node: string) => node === "END" || node === "__end__";
const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const completed = new Set(["success", "error", "interrupted", "cancelled"]);

export class GraphRuntime {
  readonly store: Store;
  readonly checkpointer: DrizzleCheckpointer;
  private readonly queue?: RunQueue;
  private readonly inline: boolean;
  private readonly graphs = new Map<string, { kind: "custom"; graph: GraphDefinition } | { kind: "compiled"; graph: CompiledGraphLike }>();

  constructor(store: Store, config: Omit<RuntimeConfig,"db"> = {}) {
    this.store = store;
    this.checkpointer = new DrizzleCheckpointer(store);
    this.queue = config.queue ? new RunQueue(config.queue) : undefined;
    this.inline = config.inline ?? !config.queue;
  }

  registerGraph(definition: GraphDefinition | CompiledGraphDefinition): void {
    if ("nodes" in definition) {
      if (!definition.nodes[definition.entrypoint]) throw new Error(`Graph ${definition.id}: entrypoint ${definition.entrypoint} is missing`);
      this.graphs.set(definition.id, { kind: "custom", graph: definition });
      return;
    }
    const graph = typeof definition.graph === "function" ? definition.graph(this.checkpointer) : definition.graph;
    // CompiledStateGraph exposes this property, even when compiled without a saver.
    graph.checkpointer = this.checkpointer;
    this.graphs.set(definition.id, { kind: "compiled", graph });
  }

  listGraphs(): string[] { return [...this.graphs.keys()]; }
  hasGraph(id: string): boolean { return this.graphs.has(id); }
  getGraph(id: string): GraphDefinition | CompiledGraphLike | null { return this.graphs.get(id)?.graph ?? null; }

  createThread(value: { id?: string; metadata?: JsonObject } = {}) { return this.store.createThread(value); }
  getThread(id: string) { return this.store.getThread(id); }
  getState(threadId: string) { return this.store.getState(threadId); }
  getHistory(threadId: string, limit?: number) { return this.store.getHistory(threadId, limit); }
  getRun(runId: string) { return this.store.getRun(runId); }
  getRuns(threadId: string, limit?: number) { return this.store.listRuns(threadId, limit); }

  async startRun(options: StartRunOptions): Promise<RunRecord> {
    if (!this.graphs.has(options.graphId)) throw new Error(`Unknown graph: ${options.graphId}`);
    if (!await this.store.getThread(options.threadId)) await this.store.createThread({ id: options.threadId });
    const previous = await this.store.getState(options.threadId);
    if (previous?.interrupts.length) throw new Error(`Thread ${options.threadId} is interrupted; call resumeRun`);
    if (!await this.store.claimThread(options.threadId, ["idle", "error"])) throw new Error(`Thread ${options.threadId} is busy`);
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
      metadata: { ...options.metadata, __resumeProvided: true },
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
    if (this.queue) await this.queue.enqueue(runId);
    if (this.inline) queueMicrotask(() => { void this.executeRun(runId).catch(() => {}); });
  }

  startWorker(): void {
    if (!this.queue) throw new Error("startWorker requires queue.redisUrl");
    this.queue.start(runId => this.executeRun(runId));
  }

  async executeRun(runId: string): Promise<void> {
    if (!await this.store.claimRun(runId)) return;
    const run = await this.store.getRun(runId);
    if (!run) return;
    const registered = this.graphs.get(run.graphId);
    if (!registered) {
      await this.fail(run, new Error(`Graph ${run.graphId} is not registered in this worker`));
      return;
    }
    await this.store.appendEvent(run.id, "run", { status: "running" });
    try {
      if (registered.kind === "custom") await this.executeCustom(run, registered.graph);
      else await this.executeCompiled(run, registered.graph);
    } catch (error) { await this.fail(run, error); }
  }

  private async fail(run: RunRecord, error: unknown): Promise<void> {
    const message = error instanceof Error ? error.message : String(error);
    await this.store.updateRun(run.id, { status: "error", error: message, leaseUntil: null });
    await this.store.updateThread(run.threadId, { status: "error" });
    await this.store.appendEvent(run.id, "error", { message });
    await this.store.appendEvent(run.id, "end", { status: "error" });
  }

  private merge(base: State, update: State, graph: GraphDefinition): State {
    const next = { ...base };
    for (const [key, value] of Object.entries(update)) {
      next[key] = graph.reducers?.[key] ? graph.reducers[key]!(base[key], value) : value;
    }
    return next;
  }

  private async executeCustom(run: RunRecord, graph: GraphDefinition): Promise<void> {
    const previous = await this.store.getState(run.threadId);
    const resuming = run.metadata.__resumeProvided === true;
    let values = previous?.values ?? {};
    let next = resuming ? previous?.next ?? [] : [graph.entrypoint];
    let step = previous?.step ?? -1;
    let parentId = previous?.id ?? null;
    if (!resuming) values = this.merge(values, stateOf(run.input), graph);
    if (!previous || !resuming) {
      step++;
      const initial = await this.store.createCheckpoint({ threadId: run.threadId, runId: run.id, graphId: graph.id,
        step, values, next, tasks: [], interrupts: [], parentId });
      parentId = initial.id;
      await this.store.appendEvent(run.id, "values", values);
    }
    const limit = graph.recursionLimit ?? 100;
    let localSteps = 0;
    while (next.length) {
      if (++localSteps > limit) throw new Error(`Graph ${graph.id} exceeded recursion limit ${limit}`);
      const current = next.shift()!;
      if (ends(current)) continue;
      const node = graph.nodes[current];
      if (!node) throw new Error(`Graph ${graph.id}: node ${current} is missing`);
      const context: NodeContext = {
        threadId: run.threadId, runId: run.id, node: current, config: run.config,
        interrupt: value => { if (resuming) return run.resume; throw new GraphInterrupted(value); },
      };
      let result: NodeResult;
      try { result = await node(values, context); }
      catch (error) {
        if (!(error instanceof GraphInterrupted)) throw error;
        await this.interruptCustom(run, graph, values, [current, ...next], step + 1, parentId, error.value);
        return;
      }
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
      const saved = await this.store.createCheckpoint({ threadId: run.threadId, runId: run.id, graphId: graph.id,
        step, values, next, tasks: [], interrupts: [], parentId });
      parentId = saved.id;
      await this.store.renewRun(run.id);
      await this.store.appendEvent(run.id, "updates", { [current]: update });
      await this.store.appendEvent(run.id, "values", values);
      if ((await this.store.getRun(run.id))?.status === "cancelled") return;
    }
    await this.store.updateRun(run.id, { status: "success", output: values, leaseUntil: null });
    await this.store.updateThread(run.threadId, { status: "idle" });
    await this.store.appendEvent(run.id, "end", { status: "success", output: values });
  }

  private async interruptCustom(run: RunRecord, graph: GraphDefinition, values: State, next: string[], step: number, parentId: string | null, value: unknown): Promise<void> {
    await this.store.createCheckpoint({ threadId: run.threadId, runId: run.id, graphId: graph.id,
      step, values, next, tasks: [{ name: next[0] }], interrupts: [{ value }], parentId });
    await this.store.updateRun(run.id, { status: "interrupted", output: values, leaseUntil: null });
    await this.store.updateThread(run.threadId, { status: "interrupted" });
    await this.store.appendEvent(run.id, "updates", { __interrupt__: [{ value }] });
    await this.store.appendEvent(run.id, "end", { status: "interrupted", output: values });
  }

  private async executeCompiled(run: RunRecord, graph: CompiledGraphLike): Promise<void> {
    const configurable = { ...(isObject(run.config.configurable) ? run.config.configurable : {}), thread_id: run.threadId };
    const config = { ...run.config, configurable, streamMode: ["updates", "values"] };
    const input = run.metadata.__resumeProvided === true ? new Command({ resume: run.resume }) : run.input;
    for await (const chunk of await graph.stream(input, config)) {
      const [mode, data] = Array.isArray(chunk) && typeof chunk[0] === "string" ? chunk as [string, unknown] : ["updates", chunk];
      await this.store.appendEvent(run.id, mode, toWire(data));
      await this.store.renewRun(run.id);
      if ((await this.store.getRun(run.id))?.status === "cancelled") return;
    }
    const snapshot = await graph.getState(config);
    const values = stateOf(toWire(snapshot.values));
    const next = Array.isArray(snapshot.next) ? snapshot.next.map(String) : [];
    const tasks = Array.isArray(snapshot.tasks) ? toWire(snapshot.tasks) as unknown[] : [];
    const interrupts = tasks.flatMap((task: unknown) => isObject(task) && Array.isArray(task.interrupts) ? task.interrupts : []);
    const previous = await this.store.getState(run.threadId);
    await this.store.createCheckpoint({ threadId: run.threadId, runId: run.id, graphId: run.graphId,
      step: (previous?.step ?? 0) + 1, values, next, tasks, interrupts,
      parentId: previous?.id ?? null });
    const status = interrupts.length || next.length ? "interrupted" : "success";
    await this.store.updateRun(run.id, { status, output: values, leaseUntil: null });
    await this.store.updateThread(run.threadId, { status: status === "success" ? "idle" : "interrupted" });
    await this.store.appendEvent(run.id, "end", { status, output: values });
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
        yield { event: item.event, data: item.data, id: String(item.seq) };
      }
      const run = await this.store.getRun(runId);
      if (!run || (completed.has(run.status) && batch.length === 0)) return;
      await sleep(options.pollMs ?? 100);
    }
  }
  async close(): Promise<void> { await this.queue?.close(); await this.store.close(); }
}

export async function createRuntime(config: RuntimeConfig): Promise<GraphRuntime> {
  return new GraphRuntime(await createStore(config.db), config);
}
