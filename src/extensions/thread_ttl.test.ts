import { expect, test } from "bun:test";
import { sql } from "drizzle-orm";
import { Annotation, END, START, StateGraph } from "@langchain/langgraph";
import { createRuntime } from "../engine/index.ts";
import { ThreadPruner } from "./thread_prune.ts";

const expired = "2020-01-01T00:00:00.000Z";

test("expired TTL waits for a pending run and then deletes its thread data", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite", url: ":memory:" }, inline: false });
  try {
    runtime.registerGraph({ id: "echo", entrypoint: "reply", nodes: { reply: value => value } });
    const thread = await runtime.store.createThread({ ttl: { ttlMinutes: 1, strategy: "delete" } });
    const run = await runtime.startRun({ threadId: thread.id, graphId: "echo", input: { value: 1 } });
    await runtime.store.exec(sql`UPDATE thread_ttl SET expires_at = ${expired} WHERE thread_id = ${thread.id}`);
    const pruner = new ThreadPruner(runtime.store);
    expect(await pruner.sweepExpired()).toEqual({ deleted: 0, pruned: 0 });
    expect(await runtime.store.getThread(thread.id)).not.toBeNull();

    await runtime.executeRun(run.id);
    expect((await runtime.store.getRun(run.id))?.status).toBe("success");
    expect(await pruner.sweepExpired()).toEqual({ deleted: 1, pruned: 0 });
    expect(await runtime.store.getThread(thread.id)).toBeNull();
    expect(await runtime.store.getThreadTtl(thread.id)).toBeNull();
    expect(await runtime.store.getRun(run.id)).toBeNull();
    expect(await runtime.store.getState(thread.id)).toBeNull();
    expect(await runtime.store.listEvents(run.id)).toEqual([]);
  } finally {
    await runtime.close();
  }
});

test("keep_latest compacts once across concurrent sweepers and re-arms TTL", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite", url: ":memory:" } });
  try {
    const state = Annotation.Root({ count: Annotation<number> });
    runtime.registerGraph({ id: "counter", graph: new StateGraph(state)
      .addNode("increment", value => ({ count: (value.count ?? 0) + 1 }))
      .addEdge(START, "increment").addEdge("increment", END).compile() });
    const thread = await runtime.store.createThread({ ttl: { ttlMinutes: 5, strategy: "keep_latest" } });
    await runtime.waitRun((await runtime.startRun({ threadId: thread.id, graphId: "counter",
      input: { count: 1 } })).id);
    await runtime.waitRun((await runtime.startRun({ threadId: thread.id, graphId: "counter",
      input: {} })).id);
    expect((await runtime.store.getHistory(thread.id, 100)).length).toBeGreaterThan(1);
    await runtime.store.exec(sql`UPDATE thread_ttl SET expires_at = ${expired} WHERE thread_id = ${thread.id}`);

    const now = Date.now();
    const [first, second] = await Promise.all([
      new ThreadPruner(runtime.store).sweepExpired(now),
      new ThreadPruner(runtime.store).sweepExpired(now),
    ]);
    expect(first.pruned + second.pruned).toBe(1);
    expect(first.deleted + second.deleted).toBe(0);
    expect((await runtime.store.getHistory(thread.id, 100)).length).toBe(1);
    const ttl = (await runtime.store.getThreadTtl(thread.id))!;
    expect(new Date(ttl.expiresAt).getTime() - now).toBe(5 * 60_000);
    expect(await new ThreadPruner(runtime.store).sweepExpired(now)).toEqual({ deleted: 0, pruned: 0 });
    expect((await runtime.waitRun((await runtime.startRun({ threadId: thread.id, graphId: "counter",
      input: {} })).id)).output).toMatchObject({ count: 4 });
  } finally {
    await runtime.close();
  }
});
