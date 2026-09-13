import { expect, test } from "bun:test";
import { sql } from "drizzle-orm";
import { Client } from "@langchain/langgraph-sdk";
import { Annotation, END, START, StateGraph } from "@langchain/langgraph";
import { Hono } from "hono";
import { createApi } from "../api/index.ts";
import { authMiddleware } from "../auth.ts";
import { createRuntime } from "../engine/index.ts";
import { createPlatformAdapter, seedDefaultAssistants } from "../platform.ts";
import { ThreadPruner } from "./thread_prune.ts";

const old = "2020-01-01T00:00:00.000Z";
const count = async (runtime: Awaited<ReturnType<typeof createRuntime>>, table: string, id: string) => {
  const column = table === "events" ? "run_id" : "thread_id";
  const rows = await runtime.store.rows<{ count: number }>(sql.raw(
    `SELECT COUNT(*) AS count FROM ${table} WHERE ${column} = '${id.replaceAll("'", "''")}'`,
  ));
  return Number(rows[0]?.count ?? 0);
};

test("stateless sweep removes finished ephemeral checkpoints but preserves pending and normal threads", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite", url: ":memory:" }, inline: false });
  try {
    const state = Annotation.Root({ count: Annotation<number> });
    runtime.registerGraph({ id: "counter", graph: new StateGraph(state)
      .addNode("increment", value => ({ count: (value.count ?? 0) + 1 }))
      .addEdge(START, "increment").addEdge("increment", END).compile() });
    await seedDefaultAssistants(runtime.store, runtime.listGraphs());
    const adapter = createPlatformAdapter(runtime, runtime.store, runtime.listGraphs());
    const context = { request: new Request("http://valida.test") };
    const pending = await adapter.runs.create(null, { assistant_id: "counter", input: { count: 1 } }, context);
    const normal = await runtime.store.createThread({ metadata: { project: "keep" } });
    const pruner = new ThreadPruner(runtime.store, { retentionMs: 60_000, sweepLimit: 2 });
    await runtime.store.exec(sql`UPDATE threads SET updated_at = ${old}`);
    expect(await pruner.sweep()).toBe(0);
    expect(await runtime.store.getThread(pending.thread_id)).not.toBeNull();

    await runtime.executeRun(pending.run_id);
    expect((await runtime.store.getRun(pending.run_id))?.status).toBe("success");
    expect(await count(runtime, "lg_checkpoints", pending.thread_id)).toBeGreaterThan(0);
    expect(await count(runtime, "events", pending.run_id)).toBeGreaterThan(0);
    await runtime.store.exec(sql`UPDATE threads SET updated_at = ${old} WHERE id = ${pending.thread_id}`);
    expect(await pruner.sweep()).toBe(1);
    expect(await runtime.store.getThread(pending.thread_id)).toBeNull();
    expect(await runtime.store.getRun(pending.run_id)).toBeNull();
    expect(await count(runtime, "lg_checkpoints", pending.thread_id)).toBe(0);
    expect(await count(runtime, "lg_writes", pending.thread_id)).toBe(0);
    expect(await count(runtime, "checkpoints", pending.thread_id)).toBe(0);
    expect(await count(runtime, "events", pending.run_id)).toBe(0);
    expect(await runtime.store.getThread(normal.id)).not.toBeNull();
  } finally {
    await runtime.close();
  }
});

test("SDK prune deletes only authorized idle threads and skips active runs", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite", url: ":memory:" }, inline: false });
  try {
    runtime.registerGraph({ id: "echo", entrypoint: "reply", nodes: { reply: value => value } });
    await seedDefaultAssistants(runtime.store, runtime.listGraphs());
    const app = new Hono();
    app.use("*", authMiddleware({
      authenticate(request) { return { identity: request.headers.get("x-user") ?? "" }; },
    }));
    app.route("/", createApi(createPlatformAdapter(runtime, runtime.store, runtime.listGraphs())));
    const client = new Client({ apiUrl: "http://valida.test", apiKey: null,
      defaultHeaders: { "x-user": "alice" },
      callerOptions: { maxRetries: 0,
        fetch: (input: RequestInfo | URL, init?: RequestInit) => app.fetch(new Request(input, init)) } });
    const alice = await client.threads.create();
    const bob = await runtime.store.createThread({ metadata: { _owner: "bob" } });
    const busy = await client.threads.create();
    const run = await client.runs.create(busy.thread_id, "echo", { input: { value: 1 } });
    expect(run.status).toBe("pending");

    expect(await client.threads.prune([alice.thread_id, bob.id, busy.thread_id])).toMatchObject({ pruned_count: 1 });
    expect(await runtime.store.getThread(alice.thread_id)).toBeNull();
    expect(await runtime.store.getThread(bob.id)).not.toBeNull();
    expect(await runtime.store.getThread(busy.thread_id)).not.toBeNull();
    expect(await client.threads.prune([busy.thread_id], { strategy: "keep_latest" }))
      .toMatchObject({ pruned_count: 0 });
    await runtime.executeRun(run.run_id);
    expect(await client.threads.prune([busy.thread_id])).toMatchObject({ pruned_count: 1 });
    expect(await runtime.store.getThread(busy.thread_id)).toBeNull();
  } finally {
    await runtime.close();
  }
});

test("SDK keep_latest preserves compiled graph state and future runs", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite", url: ":memory:" } });
  try {
    const state = Annotation.Root({ count: Annotation<number> });
    runtime.registerGraph({ id: "counter", graph: new StateGraph(state)
      .addNode("increment", value => ({ count: (value.count ?? 0) + 1 }))
      .addEdge(START, "increment").addEdge("increment", END).compile() });
    await seedDefaultAssistants(runtime.store, runtime.listGraphs());
    const app = createApi(createPlatformAdapter(runtime, runtime.store, runtime.listGraphs()));
    const client = new Client({ apiUrl: "http://valida.test", apiKey: null,
      callerOptions: { maxRetries: 0,
        fetch: (input: RequestInfo | URL, init?: RequestInit) => app.fetch(new Request(input, init)) } });
    const thread = await client.threads.create();
    await client.runs.wait(thread.thread_id, "counter", { input: { count: 1 } });
    await client.runs.wait(thread.thread_id, "counter", { input: {} });
    expect(await count(runtime, "lg_checkpoints", thread.thread_id)).toBeGreaterThan(1);
    expect(await count(runtime, "checkpoints", thread.thread_id)).toBeGreaterThan(1);

    expect(await client.threads.prune([thread.thread_id], { strategy: "keep_latest" }))
      .toMatchObject({ pruned_count: 1 });
    expect(await runtime.store.getThread(thread.thread_id)).not.toBeNull();
    expect(await count(runtime, "lg_checkpoints", thread.thread_id)).toBe(1);
    expect(await count(runtime, "checkpoints", thread.thread_id)).toBe(1);
    expect((await client.threads.getState(thread.thread_id)).values).toMatchObject({ count: 3 });
    expect((await client.threads.getHistory(thread.thread_id, { limit: 100 })).length).toBe(1);
    expect(await client.runs.wait(thread.thread_id, "counter", { input: {} })).toMatchObject({ count: 4 });
  } finally {
    await runtime.close();
  }
});

test("keep_latest leaves interrupted native pending writes usable for HITL resume", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite", url: ":memory:" } });
  try {
    const { approval } = await import("../../examples/graphs.ts");
    runtime.registerGraph({ id: "approval", graph: approval });
    await seedDefaultAssistants(runtime.store, runtime.listGraphs());
    const app = createApi(createPlatformAdapter(runtime, runtime.store, runtime.listGraphs()));
    const client = new Client({ apiUrl: "http://valida.test", apiKey: null,
      callerOptions: { maxRetries: 0,
        fetch: (input: RequestInfo | URL, init?: RequestInit) => app.fetch(new Request(input, init)) } });
    const thread = await client.threads.create();
    await client.runs.wait(thread.thread_id, "approval", { input: { proposal: "ship" } });
    expect((await client.threads.getState(thread.thread_id)).next.length).toBeGreaterThan(0);
    expect(await count(runtime, "lg_checkpoints", thread.thread_id)).toBeGreaterThan(1);
    expect(await count(runtime, "lg_writes", thread.thread_id)).toBeGreaterThan(0);
    expect(await client.threads.prune([thread.thread_id], { strategy: "keep_latest" }))
      .toMatchObject({ pruned_count: 1 });
    expect(await count(runtime, "lg_checkpoints", thread.thread_id)).toBe(1);
    expect(await count(runtime, "lg_writes", thread.thread_id)).toBeGreaterThan(0);
    const resumed = await client.runs.wait(thread.thread_id, "approval", {
      command: { resume: { decisions: [{ type: "approve" }] } },
    });
    expect(resumed).toMatchObject({ approved: true, result: "Approved: ship" });
  } finally {
    await runtime.close();
  }
});

test("pruner waits for the terminal end event before deleting", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite", url: ":memory:" }, inline: false });
  try {
    const thread = await runtime.store.createThread({ metadata: { _ephemeral: true } });
    const run = await runtime.store.createRun({ threadId: thread.id, graphId: "none", input: {} });
    await runtime.store.updateRun(run.id, { status: "success" });
    await runtime.store.exec(sql`UPDATE threads SET updated_at = ${old} WHERE id = ${thread.id}`);
    const pruner = new ThreadPruner(runtime.store, { retentionMs: 0 });
    expect(await pruner.sweep()).toBe(0);
    await runtime.store.appendEvent(run.id, "end", { status: "success" });
    expect(await pruner.sweep()).toBe(1);
  } finally {
    await runtime.close();
  }
});

if (process.env.VALIDA_TEST_POSTGRES_URL) {
  test("PostgreSQL keep_latest compacts native checkpoints transactionally", async () => {
    const runtime = await createRuntime({ db: { dialect: "postgres", url: process.env.VALIDA_TEST_POSTGRES_URL! } });
    try {
      const state = Annotation.Root({ count: Annotation<number> });
      runtime.registerGraph({ id: "counter", graph: new StateGraph(state)
        .addNode("increment", value => ({ count: (value.count ?? 0) + 1 }))
        .addEdge(START, "increment").addEdge("increment", END).compile() });
      await seedDefaultAssistants(runtime.store, runtime.listGraphs());
      const app = createApi(createPlatformAdapter(runtime, runtime.store, runtime.listGraphs()));
      const client = new Client({ apiUrl: "http://valida.test", apiKey: null,
        callerOptions: { maxRetries: 0,
          fetch: (input: RequestInfo | URL, init?: RequestInit) => app.fetch(new Request(input, init)) } });
      const thread = await client.threads.create();
      try {
        await client.runs.wait(thread.thread_id, "counter", { input: { count: 1 } });
        await client.runs.wait(thread.thread_id, "counter", { input: {} });
        expect(await count(runtime, "lg_checkpoints", thread.thread_id)).toBeGreaterThan(1);
        expect(await client.threads.prune([thread.thread_id], { strategy: "keep_latest" }))
          .toMatchObject({ pruned_count: 1 });
        expect(await count(runtime, "lg_checkpoints", thread.thread_id)).toBe(1);
        expect((await client.threads.getState(thread.thread_id)).values).toMatchObject({ count: 3 });
        expect(await client.runs.wait(thread.thread_id, "counter", { input: {} })).toMatchObject({ count: 4 });
      } finally {
        await client.threads.delete(thread.thread_id);
      }
    } finally {
      await runtime.close();
    }
  });
}
