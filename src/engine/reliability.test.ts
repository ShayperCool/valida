import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createRuntime, type GraphRuntime } from "./index.ts";

const runtimes: GraphRuntime[] = [];
const dirs: string[] = [];
const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const deferred = () => {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
};
async function database() {
  const dir = await mkdtemp(join(tmpdir(), "valida-reliability-"));
  dirs.push(dir);
  return join(dir, "db.sqlite");
}
afterEach(async () => {
  for (const runtime of runtimes.splice(0)) await runtime.close();
  for (const dir of dirs.splice(0)) await rm(dir, { recursive: true, force: true });
});

test("heartbeat keeps a long node leased across instances", async () => {
  const url = await database();
  const first = await createRuntime({ db: { dialect: "sqlite", url }, runLeaseMs: 120 });
  const second = await createRuntime({ db: { dialect: "sqlite", url }, inline: false });
  runtimes.push(first, second);
  const started = deferred(), release = deferred();
  first.registerGraph({ id: "slow", entrypoint: "node", nodes: {
    node: async () => { started.resolve(); await release.promise; return { done: true }; },
  } });
  const thread = await first.createThread();
  const run = await first.startRun({ threadId: thread.id, graphId: "slow" });
  await started.promise;
  await sleep(260);
  expect(await second.store.claimRun(run.id, 120)).toBe(false);
  release.resolve();
  expect((await first.waitRun(run.id)).status).toBe("success");
  expect((await second.getState(thread.id))?.values.done).toBe(true);
});

test("cancel on another instance stops a running node from committing its result", async () => {
  const url = await database();
  const worker = await createRuntime({ db: { dialect: "sqlite", url }, runLeaseMs: 120 });
  const api = await createRuntime({ db: { dialect: "sqlite", url }, inline: false });
  runtimes.push(worker, api);
  const started = deferred(), release = deferred();
  worker.registerGraph({ id: "slow", entrypoint: "node", nodes: {
    node: async () => { started.resolve(); await release.promise; return { late: true }; },
  } });
  const thread = await worker.createThread();
  const run = await worker.startRun({ threadId: thread.id, graphId: "slow" });
  await started.promise;
  expect((await api.store.cancelRun(run.id))?.status).toBe("cancelled");
  release.resolve();
  await sleep(80);
  expect((await api.getRun(run.id))?.status).toBe("cancelled");
  expect((await api.getState(thread.id))?.values.late).toBeUndefined();
});

test("run deadline fails a cooperative node and leaves the last checkpoint intact", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite" }, runTimeoutMs: 50, runLeaseMs: 120 });
  runtimes.push(runtime);
  runtime.registerGraph({ id: "wait", entrypoint: "node", nodes: {
    node: async (_state, context) => {
      await new Promise<void>(resolve => context.signal.addEventListener("abort", () => resolve(), { once: true }));
      return { late: true };
    },
  } });
  const thread = await runtime.createThread();
  const run = await runtime.startRun({ threadId: thread.id, graphId: "wait", input: { initial: true } });
  const terminal = await runtime.waitRun(run.id);
  expect(terminal.status).toBe("error");
  expect(terminal.error).toContain("Run timed out after 50 ms");
  expect((await runtime.getState(thread.id))?.values).toEqual({ initial: true });
  expect((await runtime.getThread(thread.id))?.status).toBe("error");
});

test("a non-cooperative node cannot overwrite a timed-out run after it returns", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite" }, runTimeoutMs: 30, runLeaseMs: 120 });
  runtimes.push(runtime);
  runtime.registerGraph({ id: "late", entrypoint: "node", nodes: {
    node: async () => { await sleep(100); return { late: true }; },
  } });
  const thread = await runtime.createThread();
  const run = await runtime.startRun({ threadId: thread.id, graphId: "late" });
  expect((await runtime.waitRun(run.id)).status).toBe("error");
  await sleep(130);
  expect((await runtime.getRun(run.id))?.status).toBe("error");
  expect((await runtime.getState(thread.id))?.values.late).toBeUndefined();
});

test("distributed worker processes durable pending run while Redis is unavailable", async () => {
  const url = await database();
  const redisUrl = "redis://127.0.0.1:29999";
  const api = await createRuntime({ db: { dialect: "sqlite", url },
    queue: { redisUrl, name: `valida-offline-${crypto.randomUUID()}` }, inline: false });
  const worker = await createRuntime({ db: { dialect: "sqlite", url },
    queue: { redisUrl, name: `valida-offline-${crypto.randomUUID()}` }, inline: false, recoveryPollMs: 20 });
  runtimes.push(api, worker);
  const graph = { id: "counter", entrypoint: "increment", nodes: {
    increment: (state: Record<string, unknown>) => ({ count: Number(state.count) + 1 }),
  } };
  api.registerGraph(graph);
  worker.registerGraph(graph);
  worker.startWorker();
  const thread = await api.createThread();
  const run = await api.startRun({ threadId: thread.id, graphId: "counter", input: { count: 0 } });
  expect((await worker.waitRun(run.id)).status).toBe("success");
  expect((await api.getState(thread.id))?.values.count).toBe(1);
});
