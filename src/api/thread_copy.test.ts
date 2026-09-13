import { expect, test } from "bun:test";
import { sql } from "drizzle-orm";
import { Annotation, END, interrupt, START, StateGraph } from "@langchain/langgraph";
import { Client } from "@langchain/langgraph-sdk";
import { createRuntime } from "../engine/index.ts";
import { createPlatformAdapter, seedDefaultAssistants } from "../platform.ts";
import { createApi } from "./index.ts";

function clientFor(app: ReturnType<typeof createApi>): Client {
  return new Client({ apiUrl: "http://valida.test", apiKey: null,
    callerOptions: { maxRetries: 0,
      fetch: (input: RequestInfo | URL, init?: RequestInit) => app.fetch(new Request(input, init)) } });
}

test("SDK thread copy preserves native and legacy checkpoint history for future runs", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite", url: ":memory:" } });
  try {
    const schema = Annotation.Root({ count: Annotation<number> });
    runtime.registerGraph({ id: "counter", graph: new StateGraph(schema)
      .addNode("increment", state => ({ count: (state.count ?? 0) + 1 }))
      .addEdge(START, "increment").addEdge("increment", END).compile() });
    await seedDefaultAssistants(runtime.store, runtime.listGraphs());
    const client = clientFor(createApi(createPlatformAdapter(runtime, runtime.store, runtime.listGraphs())));
    const source = await client.threads.create();
    expect(await client.runs.wait(source.thread_id, "counter", { input: { count: 10 } }))
      .toMatchObject({ count: 11 });
    expect(await client.runs.wait(source.thread_id, "counter", { input: {} }))
      .toMatchObject({ count: 12 });
    const copied = await client.threads.copy(source.thread_id);
    expect((await client.threads.getState(copied.thread_id)).values).toMatchObject({ count: 12 });
    const history = await client.threads.getHistory(copied.thread_id, { limit: 100 });
    expect(history.length).toBeGreaterThan(2);
    expect(history.every(entry => entry.checkpoint?.thread_id === copied.thread_id)).toBe(true);
    expect(history.every(entry => !entry.parent_checkpoint ||
      entry.parent_checkpoint.thread_id === copied.thread_id)).toBe(true);

    const originalRows = await runtime.store.rows<{ id: string }>(sql`SELECT id FROM checkpoints
      WHERE thread_id = ${source.thread_id}`);
    const copyRows = await runtime.store.rows<{ id: string; parent_id: string | null }>(sql`SELECT id, parent_id
      FROM checkpoints WHERE thread_id = ${copied.thread_id}`);
    expect(copyRows.length).toBe(originalRows.length);
    const copyIds = new Set(copyRows.map(row => row.id));
    const originalIds = new Set(originalRows.map(row => row.id));
    expect(copyRows.every(row => !originalIds.has(row.id) &&
      (!row.parent_id || copyIds.has(row.parent_id)))).toBe(true);

    expect(await client.runs.wait(copied.thread_id, "counter", { input: {} }))
      .toMatchObject({ count: 13 });
    expect((await client.threads.getState(source.thread_id)).values).toMatchObject({ count: 12 });
  } finally {
    await runtime.close();
  }
});

test("SDK thread copy resumes an interrupted compiled graph independently", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite", url: ":memory:" } });
  try {
    const schema = Annotation.Root({ proposal: Annotation<string>, approved: Annotation<boolean> });
    runtime.registerGraph({ id: "review", graph: new StateGraph(schema)
      .addNode("approve", () => ({ approved: interrupt("Approve?") as boolean }))
      .addEdge(START, "approve").addEdge("approve", END).compile() });
    await seedDefaultAssistants(runtime.store, runtime.listGraphs());
    const client = clientFor(createApi(createPlatformAdapter(runtime, runtime.store, runtime.listGraphs())));
    const source = await client.threads.create();
    const run = await client.runs.create(source.thread_id, "review", { input: { proposal: "ship" } });
    await client.runs.join(source.thread_id, run.run_id);
    const pending = await client.threads.getState(source.thread_id);
    expect(pending.next).toContain("approve");
    expect(pending.tasks.length).toBeGreaterThan(0);

    const copied = await client.threads.copy(source.thread_id);
    const copiedPending = await client.threads.getState(copied.thread_id);
    expect(copiedPending.next).toContain("approve");
    expect(copiedPending.values).toMatchObject({ proposal: "ship" });
    const sourceWrites = await runtime.store.rows<{ count: number }>(sql`SELECT COUNT(*) AS count
      FROM lg_writes WHERE thread_id = ${source.thread_id}`);
    const copiedWrites = await runtime.store.rows<{ count: number }>(sql`SELECT COUNT(*) AS count
      FROM lg_writes WHERE thread_id = ${copied.thread_id}`);
    expect(Number(copiedWrites[0]?.count)).toBe(Number(sourceWrites[0]?.count));
    expect(Number(copiedWrites[0]?.count)).toBeGreaterThan(0);

    expect(await client.runs.wait(copied.thread_id, "review", { command: { resume: true } }))
      .toMatchObject({ proposal: "ship", approved: true });
    expect((await client.threads.getState(source.thread_id)).next).toContain("approve");
    expect(await client.runs.wait(source.thread_id, "review", { command: { resume: true } }))
      .toMatchObject({ proposal: "ship", approved: true });
  } finally {
    await runtime.close();
  }
});

test("SDK thread copy refuses an active source run", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite", url: ":memory:" }, inline: false });
  try {
    runtime.registerGraph({ id: "echo", entrypoint: "reply", nodes: { reply: state => state } });
    await seedDefaultAssistants(runtime.store, runtime.listGraphs());
    const client = clientFor(createApi(createPlatformAdapter(runtime, runtime.store, runtime.listGraphs())));
    const source = await client.threads.create();
    const run = await client.runs.create(source.thread_id, "echo", { input: { text: "pending" } });
    await expect(client.threads.copy(source.thread_id)).rejects.toThrow();
    await runtime.executeRun(run.run_id);
    expect((await client.threads.copy(source.thread_id)).thread_id).toBeTruthy();
  } finally {
    await runtime.close();
  }
});
