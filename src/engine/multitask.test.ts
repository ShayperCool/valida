import { afterEach, expect, test } from "bun:test";
import { Client } from "@langchain/langgraph-sdk";
import { createApi } from "../api/index";
import { createPlatformAdapter, seedDefaultAssistants } from "../platform";
import { createRuntime, ThreadBusyError, type GraphRuntime } from "./index.js";

const open: GraphRuntime[] = [];
afterEach(async () => { for (const runtime of open.splice(0)) await runtime.close(); });

/** A graph whose single step waits until the test releases it, recording what it saw. */
async function gated() {
  const runtime = await createRuntime({ db: process.env.TEST_POSTGRES_URL
    ? { dialect: "postgres", url: process.env.TEST_POSTGRES_URL }
    : { dialect: "sqlite", url: ":memory:" } }); open.push(runtime);
  const gates = new Map<string, () => void>();
  const started: string[] = [];
  const aborted: string[] = [];
  runtime.registerGraph({
    id: "work", entrypoint: "work",
    reducers: { log: (previous, update) => [...(previous as string[] ?? []), ...(update as string[])] },
    nodes: {
      work: async (state, context) => {
        const step = String(state.step);
        started.push(step);
        await new Promise<void>((resolve, reject) => {
          gates.set(step, resolve);
          context.signal.addEventListener("abort", () => { aborted.push(step); reject(context.signal.reason); }, { once: true });
        });
        return { log: [step] };
      },
    },
    edges: { work: "__end__" },
  });
  const until = async (check: () => boolean, label: string) => {
    for (let i = 0; i < 300 && !check(); i++) await Bun.sleep(10);
    if (!check()) throw new Error(`Timed out waiting for ${label}`);
  };
  const release = async (step: string) => { await until(() => gates.has(step), `step ${step}`); gates.get(step)!(); };
  return { runtime, started, aborted, until, release };
}

test("reject refuses a second run while the thread is busy", async () => {
  const { runtime, release } = await gated();
  const thread = await runtime.createThread();
  const first = await runtime.startRun({ threadId: thread.id, graphId: "work", input: { step: "a" } });
  await expect(runtime.startRun({ threadId: thread.id, graphId: "work", input: { step: "b" } }))
    .rejects.toBeInstanceOf(ThreadBusyError);
  await release("a");
  expect((await runtime.waitRun(first.id)).status).toBe("success");
});

test("enqueue runs follow-ups in order on the state their predecessor left", async () => {
  const { runtime, started, release } = await gated();
  const thread = await runtime.createThread();
  const first = await runtime.startRun({ threadId: thread.id, graphId: "work", input: { step: "a" } });
  const second = await runtime.startRun({ threadId: thread.id, graphId: "work", input: { step: "b" }, multitaskStrategy: "enqueue" });
  const third = await runtime.startRun({ threadId: thread.id, graphId: "work", input: { step: "c" }, multitaskStrategy: "enqueue" });
  expect((await runtime.getRun(second.id))?.status).toBe("pending");
  // A queued run keeps the thread taken: a plain run is still refused between the two.
  await release("a");
  await runtime.waitRun(first.id);
  await expect(runtime.startRun({ threadId: thread.id, graphId: "work", input: { step: "x" } }))
    .rejects.toBeInstanceOf(ThreadBusyError);
  await release("b");
  await release("c");
  expect((await runtime.waitRun(third.id)).status).toBe("success");
  expect(started).toEqual(["a", "b", "c"]);
  expect((await runtime.getState(thread.id))?.values.log).toEqual(["a", "b", "c"]);
  expect((await runtime.getThread(thread.id))?.status).toBe("idle");
});

test("interrupt stops the running run at once and keeps its earlier progress", async () => {
  const { runtime, aborted, started, until, release } = await gated();
  const thread = await runtime.createThread();
  const done = await runtime.startRun({ threadId: thread.id, graphId: "work", input: { step: "a" } });
  await release("a");
  await runtime.waitRun(done.id);
  const slow = await runtime.startRun({ threadId: thread.id, graphId: "work", input: { step: "b" } });
  const queued = await runtime.startRun({ threadId: thread.id, graphId: "work", input: { step: "q" }, multitaskStrategy: "enqueue" });
  await until(() => started.includes("b"), "b to start");
  const urgent = await runtime.startRun({ threadId: thread.id, graphId: "work", input: { step: "c" }, multitaskStrategy: "interrupt" });
  expect(aborted).toContain("b");
  expect((await runtime.getRun(slow.id))?.status).toBe("cancelled");
  expect((await runtime.getRun(queued.id))?.status).toBe("cancelled");
  await release("c");
  expect((await runtime.waitRun(urgent.id)).status).toBe("success");
  expect((await runtime.getState(thread.id))?.values.log).toEqual(["a", "c"]);
});

test("rollback erases the interrupted run as if it never started", async () => {
  const { runtime, until, started, release } = await gated();
  const thread = await runtime.createThread();
  const done = await runtime.startRun({ threadId: thread.id, graphId: "work", input: { step: "a" } });
  await release("a");
  await runtime.waitRun(done.id);
  const before = await runtime.getState(thread.id);
  const slow = await runtime.startRun({ threadId: thread.id, graphId: "work", input: { step: "b" } });
  await until(() => started.includes("b"), "b to start");
  const replacement = await runtime.startRun({ threadId: thread.id, graphId: "work", input: { step: "c" }, multitaskStrategy: "rollback" });
  expect(await runtime.getRun(slow.id)).toBeNull();
  expect((await runtime.getHistory(thread.id)).some(checkpoint => checkpoint.runId === slow.id)).toBe(false);
  await release("c");
  await runtime.waitRun(replacement.id);
  expect((await runtime.getState(thread.id))?.values.log).toEqual([...(before?.values.log as string[]), "c"]);
});

test("cancel aborts a local run right away and starts the next queued one", async () => {
  const { runtime, aborted, started, until, release } = await gated();
  const thread = await runtime.createThread();
  const first = await runtime.startRun({ threadId: thread.id, graphId: "work", input: { step: "a" } });
  const next = await runtime.startRun({ threadId: thread.id, graphId: "work", input: { step: "b" }, multitaskStrategy: "enqueue" });
  await until(() => started.includes("a"), "a to start");
  await runtime.cancelRun(first.id);
  expect(aborted).toEqual(["a"]);
  await release("b");
  expect((await runtime.waitRun(next.id)).status).toBe("success");
});

test("the LangGraph SDK gets 409 on reject and can enqueue or interrupt through the API", async () => {
  const { runtime, release } = await gated();
  await seedDefaultAssistants(runtime.store, runtime.listGraphs());
  const app = createApi(createPlatformAdapter(runtime, runtime.store, runtime.listGraphs()));
  const client = new Client({ apiUrl: "http://valida.test", apiKey: null, callerOptions: { maxRetries: 0,
    fetch: (input: RequestInfo | URL, init?: RequestInit) => app.fetch(new Request(input, init)) } });
  const thread = await client.threads.create();
  const first = await client.runs.create(thread.thread_id, "work", { input: { step: "a" } });
  await expect(client.runs.create(thread.thread_id, "work", { input: { step: "b" }, multitaskStrategy: "reject" }))
    .rejects.toMatchObject({ status: 409 });
  await expect(client.runs.create(thread.thread_id, "work", { input: { step: "b" }, multitaskStrategy: "later" as never }))
    .rejects.toMatchObject({ status: 422 });
  // Without a strategy the run is queued, as on LangGraph's server.
  const queued = await client.runs.create(thread.thread_id, "work", { input: { step: "b" } });
  expect(queued.status).toBe("pending");
  expect((await client.threads.get(thread.thread_id)).status).toBe("busy");
  const urgent = await client.runs.create(thread.thread_id, "work", { input: { step: "c" }, multitaskStrategy: "interrupt" });
  await release("c");
  await client.runs.join(thread.thread_id, urgent.run_id);
  const statuses = Object.fromEntries((await client.runs.list(thread.thread_id)).map(run => [run.run_id, run.status]));
  expect(statuses).toEqual({ [first.run_id]: "interrupted", [queued.run_id]: "interrupted", [urgent.run_id]: "success" });

  // Cancel answers 202, then 404 once nothing is left to cancel; bulk cancel takes thread_id with run_ids.
  const next = await client.runs.create(thread.thread_id, "work", { input: { step: "d" } });
  const later = await client.runs.create(thread.thread_id, "work", { input: { step: "e" } });
  const cancel = (path: string, init?: RequestInit) => app.fetch(new Request(`http://valida.test${path}`, { method: "POST", ...init }));
  expect((await cancel(`/threads/${thread.thread_id}/runs/${next.run_id}/cancel`)).status).toBe(202);
  expect((await cancel(`/threads/${thread.thread_id}/runs/${next.run_id}/cancel`)).status).toBe(404);
  const bulk = (body: unknown) => cancel("/runs/cancel?action=rollback",
    { headers: { "content-type": "application/json" }, body: JSON.stringify(body) });
  expect((await bulk({ thread_id: thread.thread_id, status: "all" })).status).toBe(422);
  expect((await bulk({ thread_id: thread.thread_id, run_ids: [later.run_id] })).status).toBe(204);
  expect((await bulk({ thread_id: thread.thread_id, run_ids: [later.run_id] })).status).toBe(404);
  expect((await client.threads.get(thread.thread_id)).status).toBe("idle");
});
