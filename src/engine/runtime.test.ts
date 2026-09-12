import { afterEach, expect, test } from "bun:test";
import { Annotation, END, interrupt, START, StateGraph } from "@langchain/langgraph";
import { AIMessage, AIMessageChunk, HumanMessage } from "@langchain/core/messages";
import { MessagesAnnotation } from "@langchain/langgraph";
import { FakeStreamingChatModel } from "@langchain/core/utils/testing";
import { tool } from "@langchain/core/tools";
import { z } from "zod";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createRuntime, type GraphRuntime } from "./index.js";

const open: GraphRuntime[] = [];
const dirs: string[] = [];
afterEach(async () => {
  for (const runtime of open.splice(0)) await runtime.close();
  for (const dir of dirs.splice(0)) await rm(dir, { recursive: true, force: true });
});

test("custom graph persists interrupt, resume, history, and replayable events", async () => {
  const dir = await mkdtemp(join(tmpdir(), "valida-engine-")); dirs.push(dir);
  const url = join(dir, "db.sqlite");
  const runtime = await createRuntime({ db: { dialect: "sqlite", url } }); open.push(runtime);
  runtime.registerGraph({
    id: "approval", entrypoint: "calculate",
    nodes: {
      calculate: state => ({ result: Number(state.input) * 2 }),
      approve: (_state, ctx) => ({ approved: ctx.interrupt({ question: "approve?" }) }),
    },
    edges: { calculate: "approve", approve: END },
  });
  const thread = await runtime.createThread();
  const first = await runtime.startRun({ threadId: thread.id, graphId: "approval", input: { input: 4 } });
  expect((await runtime.waitRun(first.id)).status).toBe("interrupted");
  expect((await runtime.getState(thread.id))?.values.result).toBe(8);
  expect((await runtime.getState(thread.id))?.interrupts).toEqual([{ value: { question: "approve?" } }]);
  const replay = [];
  for await (const event of runtime.stream(first.id)) replay.push(event.event);
  expect(replay).toContain("end");
  await runtime.close(); open.splice(open.indexOf(runtime), 1);

  const resumedRuntime = await createRuntime({ db: { dialect: "sqlite", url } }); open.push(resumedRuntime);
  resumedRuntime.registerGraph({
    id: "approval", entrypoint: "calculate",
    nodes: {
      calculate: state => ({ result: Number(state.input) * 2 }),
      approve: (_state, ctx) => ({ approved: ctx.interrupt({ question: "approve?" }) }),
    },
    edges: { calculate: "approve", approve: END },
  });
  const resumed = await resumedRuntime.resumeRun({ threadId: thread.id, resume: true });
  expect((await resumedRuntime.waitRun(resumed.id)).status).toBe("success");
  expect((await resumedRuntime.getState(thread.id))?.values).toMatchObject({ input: 4, result: 8, approved: true });
  expect((await resumedRuntime.getHistory(thread.id)).length).toBeGreaterThan(3);
});

test("compiled StateGraph uses durable checkpointer for interrupt and resume", async () => {
  const dir = await mkdtemp(join(tmpdir(), "valida-langgraph-")); dirs.push(dir);
  const url = join(dir, "db.sqlite");
  const compile = () => {
    const schema = Annotation.Root({ value: Annotation<number>, approved: Annotation<boolean> });
    return new StateGraph(schema)
      .addNode("ask", state => ({ value: state.value + 1, approved: interrupt("approve") as boolean }))
      .addEdge(START, "ask").addEdge("ask", END).compile();
  };
  const runtime = await createRuntime({ db: { dialect: "sqlite", url } }); open.push(runtime);
  runtime.registerGraph({ id: "langgraph", graph: compile() });
  const thread = await runtime.createThread();
  const first = await runtime.startRun({ threadId: thread.id, graphId: "langgraph", input: { value: 1 } });
  expect((await runtime.waitRun(first.id)).status).toBe("interrupted");
  const v2 = [];
  for await (const item of runtime.streamV2(first.id)) v2.push(item.event);
  expect(v2.some(event => event.method === "input.requested" &&
    (event.params.data as { payload?: unknown }).payload === "approve")).toBe(true);
  expect(v2.at(-1)?.method).toBe("lifecycle");
  expect((v2.at(-1)?.params.data as { event?: string }).event).toBe("interrupted");
  await runtime.close(); open.splice(open.indexOf(runtime), 1);
  const resumedRuntime = await createRuntime({ db: { dialect: "sqlite", url } }); open.push(resumedRuntime);
  resumedRuntime.registerGraph({ id: "langgraph", graph: compile() });
  const second = await resumedRuntime.resumeRun({ threadId: thread.id, resume: true });
  expect((await resumedRuntime.waitRun(second.id)).status).toBe("success");
  expect((await resumedRuntime.getState(thread.id))?.values).toMatchObject({ value: 2, approved: true });
});

test("compiled graph exposes Agent Protocol messages in events and saved state", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite" } }); open.push(runtime);
  const graph = new StateGraph(MessagesAnnotation)
    .addNode("reply", () => ({ messages: [new AIMessage("Echo: hello")] }))
    .addEdge(START, "reply").addEdge("reply", END).compile();
  runtime.registerGraph({ id: "chat", graph });
  const thread = await runtime.createThread();
  const run = await runtime.startRun({ threadId: thread.id, graphId: "chat",
    input: { messages: [new HumanMessage("hello")] } });
  expect((await runtime.waitRun(run.id)).status).toBe("success");
  const messages = (await runtime.getState(thread.id))?.values.messages as Array<Record<string, unknown>>;
  expect(messages.map(message => message.type)).toEqual(["human", "ai"]);
  expect(messages.map(message => message.content)).toEqual(["hello", "Echo: hello"]);
  expect(messages.every(message => typeof message.id === "string")).toBe(true);
  expect(messages.every(message => !('lc' in message))).toBe(true);
  expect(messages.every(message => Object.keys(message).every(key => !key.startsWith("lc_")))).toBe(true);
  const events = await runtime.store.listEvents(run.id);
  const valueEvent = events.find(event => event.event === "values" &&
    Array.isArray((event.data as Record<string, unknown>)?.messages) &&
    ((event.data as { messages: unknown[] }).messages).length === 2);
  expect(valueEvent).toBeDefined();
  expect((valueEvent!.data as { messages: Array<{ type: string }> }).messages[1]?.type).toBe("ai");
  const update = events.find(event => event.event === "updates" &&
    (event.data as Record<string, unknown>)?.reply);
  expect((update?.data as { reply: { messages: Array<{ type: string }> } }).reply.messages[0]?.type).toBe("ai");
});

test("standalone runtime resumes an expired run from its last checkpoint", async () => {
  const dir = await mkdtemp(join(tmpdir(), "valida-recovery-")); dirs.push(dir);
  const url = join(dir, "db.sqlite");
  const firstRuntime = await createRuntime({ db: { dialect: "sqlite", url } }); open.push(firstRuntime);
  const thread = await firstRuntime.createThread();
  const run = await firstRuntime.store.createRun({ threadId: thread.id, graphId: "recover", input: { count: 0 } });
  await firstRuntime.store.createCheckpoint({ threadId: thread.id, runId: run.id, graphId: "recover",
    step: 1, values: { count: 2 }, next: ["finish"], tasks: [], interrupts: [], parentId: null });
  await firstRuntime.store.updateRun(run.id, { status: "running", leaseUntil: "2000-01-01T00:00:00.000Z" });
  await firstRuntime.close(); open.splice(open.indexOf(firstRuntime), 1);

  const runtime = await createRuntime({ db: { dialect: "sqlite", url } }); open.push(runtime);
  runtime.registerGraph({ id: "recover", entrypoint: "calculate", nodes: {
    calculate: () => { throw new Error("should not replay completed node"); },
    finish: state => ({ count: Number(state.count) + 1 }),
  } });
  await runtime.recoverPendingRuns();
  expect((await runtime.waitRun(run.id)).status).toBe("success");
  expect((await runtime.getState(thread.id))?.values.count).toBe(3);
});

test("native checkpoint history supports editing the first human turn", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite" } }); open.push(runtime);
  const graph = new StateGraph(MessagesAnnotation)
    .addNode("reply", state => {
      const last = [...state.messages].reverse().find(message => message.getType() === "human");
      return { messages: [new AIMessage(`Echo: ${last?.content}`)] };
    })
    .addEdge(START, "reply").addEdge("reply", END).compile();
  runtime.registerGraph({ id: "editable", graph });
  const thread = await runtime.createThread();
  const first = await runtime.startRun({ threadId: thread.id, graphId: "editable",
    input: { messages: [new HumanMessage("first")] } });
  expect((await runtime.waitRun(first.id)).status).toBe("success");
  const history = await runtime.getGraphHistory(thread.id);
  expect(history.length).toBeGreaterThan(1);
  const beforeReply = history.find(snapshot => {
    const messages = snapshot.values.messages as unknown[] | undefined;
    return messages?.length === 1 && snapshot.next.includes("reply");
  });
  expect(beforeReply?.config?.checkpoint_id).toBeDefined();
  const historical = await runtime.getGraphState(thread.id, beforeReply!.config!.checkpoint_id);
  expect((historical?.values.messages as Array<{ content: string }>)[0]?.content).toBe("first");
  const originalId = (historical!.values.messages as Array<{ id: string }>)[0]!.id;
  const branch = await runtime.startRun({ threadId: thread.id, graphId: "editable",
    input: { messages: [new HumanMessage({ id: originalId, content: "edited" })] },
    config: { configurable: { checkpoint_id: beforeReply!.config!.checkpoint_id } } });
  expect((await runtime.waitRun(branch.id)).status).toBe("success");
  const latest = await runtime.getGraphState(thread.id);
  expect((latest?.values.messages as Array<{ content: string }>).map(message => message.content))
    .toEqual(["edited", "Echo: edited"]);
  expect(latest?.parentConfig?.checkpoint_id).toBeDefined();
  expect((await runtime.getGraphHistory(thread.id)).some(snapshot =>
    snapshot.parentConfig?.checkpoint_id === beforeReply!.config!.checkpoint_id &&
    snapshot.metadata.source === "input")).toBe(true);

  const beforeHumanId = beforeReply!.parentConfig!.checkpoint_id!;
  expect((await runtime.getGraphState(thread.id, beforeHumanId))?.values.messages).toEqual([]);
  const edit = await runtime.startRun({ threadId: thread.id, graphId: "editable",
    input: { messages: [new HumanMessage("new first turn")] },
    config: { configurable: { checkpoint_id: beforeHumanId } } });
  expect((await runtime.waitRun(edit.id)).status).toBe("success");
  expect(((await runtime.getGraphState(thread.id))?.values.messages as Array<{ content: string }>).map(message => message.content))
    .toEqual(["new first turn", "Echo: new first turn"]);

  const refresh = await runtime.startRun({ threadId: thread.id, graphId: "editable", input: {},
    config: { configurable: { checkpoint_id: beforeReply!.config!.checkpoint_id } } });
  expect((await runtime.waitRun(refresh.id)).status).toBe("success");
  expect(((await runtime.getGraphState(thread.id))?.values.messages as Array<{ content: string }>).map(message => message.content))
    .toEqual(["first", "Echo: first"]);
});

test("v2 stream persists native content-block token events without a remote model", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite" } }); open.push(runtime);
  const model = new FakeStreamingChatModel({ chunks: [new AIMessageChunk("He"), new AIMessageChunk("llo")] });
  const graph = new StateGraph(MessagesAnnotation)
    .addNode("reply", async state => ({ messages: [await model.invoke(state.messages)] }))
    .addEdge(START, "reply").addEdge("reply", END).compile();
  runtime.registerGraph({ id: "tokens", graph });
  const thread = await runtime.createThread();
  const run = await runtime.startRun({ threadId: thread.id, graphId: "tokens",
    input: { messages: [new HumanMessage("hi")] } });
  expect((await runtime.waitRun(run.id)).status).toBe("success");
  const events = [];
  for await (const item of runtime.streamV2(run.id)) events.push(item.event);
  const deltas = events.filter(event => event.method === "messages" &&
    (event.params.data as { event?: string }).event === "content-block-delta");
  expect(deltas.map(event => (event.params.data as { delta: { text: string } }).delta.text)).toEqual(["He", "llo"]);
  expect(deltas.every(event => event.params.namespace.length > 0 && event.params.node === "reply")).toBe(true);
  expect(events.some(event => event.method === "lifecycle" &&
    (event.params.data as { event?: string }).event === "completed")).toBe(true);
  expect(events.some(event => event.method === "checkpoints")).toBe(true);
  const legacy = await runtime.store.listEvents(run.id);
  expect(legacy.some(event => event.event === "values")).toBe(true);
  const legacyStream = [];
  for await (const item of runtime.stream(run.id)) legacyStream.push(item.event);
  expect(legacyStream).not.toContain("v2");
  expect(((await runtime.getState(thread.id))?.values.messages as Array<{ content: Array<{ type: string; text: string }> }>).at(-1)?.content)
    .toEqual([{ type: "text", text: "Hello" }]);
});

test("v2 stream keeps native tool lifecycle and subgraph namespaces", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite" } }); open.push(runtime);
  const state = Annotation.Root({ n: Annotation<number>, result: Annotation<string> });
  const add = tool(async ({ a, b }) => String(a + b), {
    name: "add", description: "Add two numbers", schema: z.object({ a: z.number(), b: z.number() }),
  });
  const child = new StateGraph(state)
    .addNode("calculate", async input => ({ n: input.n + 1, result: await add.invoke({ a: input.n, b: 1 }) }))
    .addEdge(START, "calculate").addEdge("calculate", END).compile();
  const graph = new StateGraph(state)
    .addNode("child", child).addEdge(START, "child").addEdge("child", END).compile();
  runtime.registerGraph({ id: "nested", graph });
  const thread = await runtime.createThread();
  const run = await runtime.startRun({ threadId: thread.id, graphId: "nested", input: { n: 2 } });
  expect((await runtime.waitRun(run.id)).status).toBe("success");
  const events = [];
  for await (const item of runtime.streamV2(run.id)) events.push(item.event);
  expect(events.some(event => event.method === "tools" &&
    (event.params.data as { event?: string }).event === "tool-started")).toBe(true);
  expect(events.some(event => event.method === "tools" &&
    (event.params.data as { event?: string }).event === "tool-finished")).toBe(true);
  expect(events.some(event => event.params.namespace[0]?.startsWith("child:"))).toBe(true);
  expect((await runtime.getGraphState(thread.id))?.values).toMatchObject({ n: 3, result: "3" });
});
