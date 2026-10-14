import { afterEach, expect, test } from "bun:test";
import { AIMessageChunk, HumanMessage } from "@langchain/core/messages";
import { FakeStreamingChatModel } from "@langchain/core/utils/testing";
import { END, MessagesAnnotation, START, StateGraph, type ProtocolEvent } from "@langchain/langgraph";
import { createRuntime, type GraphRuntime } from "./index.js";
import { RunEventLog } from "./events.js";

const open: GraphRuntime[] = [];
afterEach(async () => { for (const runtime of open.splice(0)) await runtime.close(); });

async function chat() {
  const runtime = await createRuntime({ db: { dialect: "sqlite", url: ":memory:" } }); open.push(runtime);
  const model = new FakeStreamingChatModel({ chunks: [new AIMessageChunk("He"), new AIMessageChunk("llo")] });
  runtime.registerGraph({ id: "chat", graph: new StateGraph(MessagesAnnotation)
    .addNode("reply", async state => ({ messages: [await model.invoke(state.messages)] }))
    .addEdge(START, "reply").addEdge("reply", END).compile() });
  return runtime;
}
const method = (item: { event: string; data: unknown }) =>
  item.event === "v2" ? `${(item.data as ProtocolEvent).method}:${String(((item.data as ProtocolEvent).params.data as { event?: string })?.event ?? "")}` : item.event;

test("a run that asked for no replay modes streams everything live but stores only lifecycle and deltas", async () => {
  const runtime = await chat();
  const thread = await runtime.createThread();
  const run = await runtime.startRun({ threadId: thread.id, graphId: "chat",
    input: { messages: [new HumanMessage("hi")] }, metadata: { __stream_modes: [] } });
  const live: string[] = [];
  for await (const item of runtime.streamAll(run.id)) live.push(method(item));
  expect(live).toContain("messages:content-block-delta");
  expect(live).toContain("values:");
  const stored = (await runtime.store.listEvents(run.id, 0, 1000)).map(method);
  expect(stored).not.toContain("messages:content-block-delta");
  expect(stored).not.toContain("tasks:");
  // Only the final state is kept, once; the thread state is the record of every step.
  expect(stored.filter(name => name === "values:")).toHaveLength(1);
  expect(stored).toContain("messages:message-finish");
  expect(stored.at(-1)).toBe("end");
  expect((await runtime.getRun(run.id))?.output).toBeNull();
});

test("a run created for the values and messages modes keeps them for replay", async () => {
  const runtime = await chat();
  const thread = await runtime.createThread();
  const run = await runtime.startRun({ threadId: thread.id, graphId: "chat",
    input: { messages: [new HumanMessage("hi")] }, metadata: { __stream_modes: ["values", "messages"] } });
  await runtime.waitRun(run.id);
  const stored = (await runtime.store.listEvents(run.id, 0, 1000)).map(method);
  expect(stored.filter(name => name === "values:").length).toBeGreaterThan(1);
  expect(stored).toContain("messages:content-block-delta");
});

test("live readers follow appends without polling and replay from where they resume", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite", url: ":memory:" } }); open.push(runtime);
  const log = new RunEventLog(runtime.store, { flushMs: 5 });
  await runtime.store.appendEvent("r", "metadata", { run_id: "r" });
  await log.open("r");
  let finished = false;
  const seen: string[] = [];
  const reader = (async () => {
    for await (const item of log.read("r", { finished: async () => finished })) seen.push(`${item.seq}:${item.event}`);
  })();
  await log.append("r", "a", 1);
  await log.append("r", "token", 2, false);
  await log.append("r", "b", 3);
  await Bun.sleep(20);
  expect(seen).toEqual(["1:metadata", "2:a", "3:token", "4:b"]);
  await log.close("r");
  finished = true;
  await reader;
  expect((await runtime.store.listEvents("r")).map(item => item.event)).toEqual(["metadata", "a", "b"]);
  const resumed: number[] = [];
  for await (const item of log.read("r", { after: 2, finished: async () => true })) resumed.push(item.seq);
  expect(resumed).toEqual([3, 4]);
});

test("the live buffer is bounded and older events fall back to the database", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite", url: ":memory:" } }); open.push(runtime);
  const log = new RunEventLog(runtime.store, { maxEvents: 3 });
  await log.open("r");
  for (let i = 1; i <= 6; i++) await log.append("r", i % 2 ? "kept" : "live", i, i % 2 === 1);
  await log.close("r");
  const seqs: number[] = [];
  for await (const item of log.read("r", { finished: async () => true })) seqs.push(item.seq);
  // 1 and 3 come from the database, 4..6 from memory; the live-only event 2 is gone, as after a TTL.
  expect(seqs).toEqual([1, 3, 4, 5, 6]);
});
