import { afterEach, expect, test } from "bun:test";
import { Annotation, END, interrupt, START, StateGraph } from "@langchain/langgraph";
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
  await runtime.close(); open.splice(open.indexOf(runtime), 1);
  const resumedRuntime = await createRuntime({ db: { dialect: "sqlite", url } }); open.push(resumedRuntime);
  resumedRuntime.registerGraph({ id: "langgraph", graph: compile() });
  const second = await resumedRuntime.resumeRun({ threadId: thread.id, resume: true });
  expect((await resumedRuntime.waitRun(second.id)).status).toBe("success");
  expect((await resumedRuntime.getState(thread.id))?.values).toMatchObject({ value: 2, approved: true });
});
