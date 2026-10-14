import { afterEach, expect, test } from "bun:test";
import { AIMessage, HumanMessage, ToolMessage, type BaseMessage } from "@langchain/core/messages";
import {
  DeltaValue, END, MessagesDeltaValue, MessagesValue, Send, START, StateGraph, StateSchema, messagesDeltaReducer,
} from "@langchain/langgraph";
import { z } from "zod";
import { sql } from "drizzle-orm";
import { createRuntime, type GraphRuntime } from "./index.js";
import { ThreadPruner } from "../extensions/thread_prune.js";

const open: GraphRuntime[] = [];
afterEach(async () => { for (const runtime of open.splice(0)) await runtime.close(); });

const bytes = async (runtime: GraphRuntime, table: string, threadId: string) => Number((await runtime.store.rows<{ n: number }>(
  sql`SELECT COALESCE(SUM(LENGTH(blob)), 0) AS n FROM ${sql.raw(table)} WHERE thread_id = ${threadId}`))[0]!.n);
const rows = async (runtime: GraphRuntime, table: string, threadId: string) => Number((await runtime.store.rows<{ n: number }>(
  sql`SELECT COUNT(*) AS n FROM ${sql.raw(table)} WHERE thread_id = ${threadId}`))[0]!.n);

/**
 * The shape of langchain's createAgent (tool dispatch "v2"): every step the model answers with a
 * tool call and each call is sent to the tools node with a full copy of the state.
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
function agent(messages: any, steps: number) {
  const State = new StateSchema({ messages, step: z.number().default(0) });
  const text = "x".repeat(2_000);
  return new StateGraph(State)
    .addNode("model", state => ({ step: state.step + 1, messages: [new AIMessage({ content: `${text} ${state.step}`,
      tool_calls: [{ id: `call-${state.step}`, name: "work", args: {} }] })] }))
    .addNode("tools", state => ({ messages: [new ToolMessage({ content: `${text} result ${state.step}`,
      tool_call_id: `call-${state.step - 1}` })] }))
    .addEdge(START, "model")
    .addConditionalEdges("model", state => state.step >= steps ? END
      : ((state.messages as BaseMessage[]).at(-1) as AIMessage).tool_calls!.map(call => new Send("tools", { ...state, lg_tool_call: call })))
    .addEdge("tools", "model")
    .compile();
}

test("a long agent stores each message once, though every step and Send repeats the history", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite", url: ":memory:" } }); open.push(runtime);
  runtime.registerGraph({ id: "agent", graph: agent(MessagesValue, 40) });
  const thread = await runtime.createThread();
  const run = await runtime.startRun({ threadId: thread.id, graphId: "agent", input: { messages: [new HumanMessage("go")] }, config: { recursionLimit: 1000 } });
  expect((await runtime.waitRun(run.id)).status).toBe("success");
  const state = await runtime.getGraphState(thread.id);
  const messages = state!.values.messages as Array<{ type: string; content: string }>;
  expect(messages).toHaveLength(80);
  expect(messages.at(-1)!.content).toEndWith(" 39");
  // 80 messages, about 2 KB each; full copies per step and per Send would be over 6 MB. A node's write
  // carries a message before the reducer assigns its id, so a message is stored at most twice.
  expect(await rows(runtime, "lg_messages", thread.id)).toBeLessThanOrEqual(160);
  const total = await bytes(runtime, "lg_messages", thread.id) + await bytes(runtime, "lg_blobs", thread.id)
    + await bytes(runtime, "lg_checkpoint_writes", thread.id);
  expect(total).toBeLessThan(1_000_000);
  // A full history read back through the saver is identical, Send packets included.
  const history = await runtime.getGraphHistory(thread.id, 1000);
  expect(history.length).toBeGreaterThan(80);
  const tuple = await runtime.checkpointer.getTuple({ configurable: { thread_id: thread.id } });
  expect((tuple!.checkpoint.channel_values.messages as BaseMessage[]).map(message => message.content))
    .toEqual(messages.map(message => message.content));
});

test("DeltaChannel messages keep snapshots at the configured frequency and survive keep_latest", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite", url: ":memory:" } }); open.push(runtime);
  // MessagesDeltaValue with its own snapshot cadence, as Python deepagents does (snapshot_frequency=50).
  const messages = new DeltaValue(MessagesDeltaValue.valueSchema, { inputSchema: MessagesDeltaValue.inputSchema,
    reducer: messagesDeltaReducer, snapshotFrequency: 10, jsonSchemaExtra: MessagesDeltaValue.jsonSchemaExtra });
  runtime.registerGraph({ id: "agent", graph: agent(messages, 15) });
  const thread = await runtime.createThread();
  const first = await runtime.startRun({ threadId: thread.id, graphId: "agent", input: { messages: [new HumanMessage("go")] }, config: { recursionLimit: 1000 } });
  expect((await runtime.waitRun(first.id)).status).toBe("success");
  // Only snapshots store the message list; the other versions are deltas replayed from writes.
  const stored = await runtime.store.rows<{ type: string }>(sql`SELECT type FROM lg_blobs
    WHERE thread_id = ${thread.id} AND channel = ${"messages"}`);
  expect(stored.filter(row => row.type !== "empty").length).toBeLessThan(stored.length / 3);
  const contents = async () => ((await runtime.getGraphState(thread.id))!.values.messages as Array<{ content: string }>)
    .map(message => message.content.slice(-3));
  expect(await contents()).toHaveLength(30);

  await runtime.store.updateThread(thread.id, { ttl: { ttlMinutes: 1, strategy: "keep_latest" } });
  expect((await new ThreadPruner(runtime.store).sweepExpired(Date.now() + 120_000)).pruned).toBe(1);
  expect(await rows(runtime, "lg_checkpoints_v2", thread.id)).toBeLessThan(10);
  expect(await contents()).toHaveLength(30);
  const next = await runtime.startRun({ threadId: thread.id, graphId: "agent", input: { messages: [new HumanMessage("again")] }, config: { recursionLimit: 1000 } });
  expect((await runtime.waitRun(next.id)).status).toBe("success");
  expect((await contents()).length).toBeGreaterThan(30);
});

test("rollback removes what the cancelled run stored, messages included", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite", url: ":memory:" } }); open.push(runtime);
  let release!: () => void;
  const gate = new Promise<void>(resolve => { release = resolve; });
  const State = new StateSchema({ messages: MessagesValue });
  runtime.registerGraph({ id: "slow", graph: new StateGraph(State)
    .addNode("first", () => ({ messages: [new AIMessage("written before the wait")] }))
    .addNode("wait", async () => { await gate; return {}; })
    .addEdge(START, "first").addEdge("first", "wait").addEdge("wait", END).compile() });
  const thread = await runtime.createThread();
  const done = await runtime.startRun({ threadId: thread.id, graphId: "slow", input: { messages: [new HumanMessage("one")] } });
  release();
  await runtime.waitRun(done.id);
  const before = await Promise.all(["lg_checkpoints_v2", "lg_blobs", "lg_checkpoint_writes", "lg_messages"]
    .map(table => rows(runtime, table, thread.id)));
  const pending = new Promise<void>(() => {});
  runtime.registerGraph({ id: "slow", graph: new StateGraph(State)
    .addNode("first", () => ({ messages: [new AIMessage("rolled back")] }))
    .addNode("wait", async () => { await pending; return {}; })
    .addEdge(START, "first").addEdge("first", "wait").addEdge("wait", END).compile() });
  const run = await runtime.startRun({ threadId: thread.id, graphId: "slow", input: { messages: [new HumanMessage("two")] } });
  for (let i = 0; i < 200 && (await rows(runtime, "lg_messages", thread.id)) === before[3]; i++) await Bun.sleep(10);
  await runtime.cancelRun(run.id, "rollback");
  const after = await Promise.all(["lg_checkpoints_v2", "lg_blobs", "lg_checkpoint_writes", "lg_messages"]
    .map(table => rows(runtime, table, thread.id)));
  expect(after).toEqual(before);
});
