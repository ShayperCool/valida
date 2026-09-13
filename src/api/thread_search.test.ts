import { expect, test } from "bun:test";
import { sql } from "drizzle-orm";
import { Annotation, END, START, StateGraph } from "@langchain/langgraph";
import { Client } from "@langchain/langgraph-sdk";
import { createRuntime } from "../engine/index.ts";
import { createPlatformAdapter, seedDefaultAssistants } from "../platform.ts";
import { createApi } from "./index.ts";

test("SDK thread search filters IDs and state values, then sorts and paginates deterministically", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite", url: ":memory:" } });
  try {
    const schema = Annotation.Root({ count: Annotation<number> });
    runtime.registerGraph({ id: "counter", graph: new StateGraph(schema)
      .addNode("increment", value => ({ count: (value.count ?? 0) + 1 }))
      .addEdge(START, "increment").addEdge("increment", END).compile() });
    await seedDefaultAssistants(runtime.store, runtime.listGraphs());
    const app = createApi(createPlatformAdapter(runtime, runtime.store, runtime.listGraphs()));
    const client = new Client({ apiUrl: "http://valida.test", apiKey: null,
      callerOptions: { maxRetries: 0,
        fetch: (input: RequestInfo | URL, init?: RequestInit) => app.fetch(new Request(input, init)) } });
    const alpha = await client.threads.create({ metadata: { team: "a" } });
    const beta = await client.threads.create({ metadata: { team: "a" } });
    const gamma = await client.threads.create({ metadata: { team: "b" } });
    await client.runs.wait(alpha.thread_id, "counter", { input: { count: 1 } });
    await client.runs.wait(beta.thread_id, "counter", { input: { count: 5 } });
    await runtime.store.exec(sql`UPDATE threads SET created_at = ${"2020-01-01T00:00:00.000Z"},
      updated_at = ${"2020-01-01T00:00:00.000Z"} WHERE id = ${alpha.thread_id}`);
    await runtime.store.exec(sql`UPDATE threads SET created_at = ${"2021-01-01T00:00:00.000Z"},
      updated_at = ${"2021-01-01T00:00:00.000Z"} WHERE id = ${beta.thread_id}`);
    await runtime.store.exec(sql`UPDATE threads SET created_at = ${"2019-01-01T00:00:00.000Z"},
      updated_at = ${"2019-01-01T00:00:00.000Z"} WHERE id = ${gamma.thread_id}`);
    await runtime.store.exec(sql`UPDATE checkpoints SET created_at = ${"2020-06-01T00:00:00.000Z"}
      WHERE thread_id = ${alpha.thread_id}`);
    await runtime.store.exec(sql`UPDATE checkpoints SET created_at = ${"2022-06-01T00:00:00.000Z"}
      WHERE thread_id = ${beta.thread_id}`);

    expect((await client.threads.search({ ids: [beta.thread_id] })).map(thread => thread.thread_id))
      .toEqual([beta.thread_id]);
    expect(await client.threads.search({ ids: [] })).toEqual([]);
    expect((await client.threads.search({ values: { count: 2 } })).map(thread => thread.thread_id))
      .toEqual([alpha.thread_id]);
    expect(await client.threads.count({ values: { count: 2 } })).toBe(1);
    expect((await client.threads.search()).map(thread => thread.thread_id))
      .toEqual([beta.thread_id, alpha.thread_id, gamma.thread_id]);
    expect((await client.threads.search({ sortBy: "created_at", sortOrder: "asc" }))
      .map(thread => thread.thread_id)).toEqual([gamma.thread_id, alpha.thread_id, beta.thread_id]);
    expect((await client.threads.search({ sortBy: "created_at", sortOrder: "desc",
      offset: 1, limit: 1 })).map(thread => thread.thread_id)).toEqual([alpha.thread_id]);
    const byState = await client.threads.search({ sortBy: "state_updated_at", sortOrder: "asc" });
    expect(byState.map(thread => thread.thread_id))
      .toEqual([gamma.thread_id, alpha.thread_id, beta.thread_id]);
    expect(byState[1]?.state_updated_at).toBe("2020-06-01T00:00:00.000Z");
    expect((await client.threads.search({ metadata: { team: "a" }, sortBy: "thread_id",
      sortOrder: "asc" })).map(thread => thread.thread_id))
      .toEqual([alpha.thread_id, beta.thread_id].sort());

    const invalid = await app.request("/threads/search", { method: "POST",
      headers: { "content-type": "application/json" }, body: JSON.stringify({ sort_by: "unknown" }) });
    expect(invalid.status).toBe(422);
  } finally {
    await runtime.close();
  }
});
