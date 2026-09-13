import { expect, test } from "bun:test";
import { sql } from "drizzle-orm";
import { Client } from "@langchain/langgraph-sdk";
import { Hono } from "hono";
import { authMiddleware } from "../auth.ts";
import { createRuntime } from "../engine/index.ts";
import { createPlatformAdapter } from "../platform.ts";
import { ThreadPruner } from "../extensions/thread_prune.ts";
import { resolveThreadTtlPolicy } from "../extensions/thread_ttl_config.ts";
import { createApi } from "./index.ts";

test("SDK thread TTL uses minutes, updates expiry, and respects auth", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite", url: ":memory:" } });
  try {
    const app = new Hono();
    app.use("*", authMiddleware({
      authenticate(request) { return { identity: request.headers.get("x-user") ?? "" }; },
      authorize(context, value) {
        if (context.action === "create" && context.user.identity === "alice" &&
          (value.metadata as { preset?: string } | undefined)?.preset === "short") {
          return { ...value, ttl: { ttl: 0.5, strategy: "delete" } };
        }
        return true;
      },
    }));
    app.route("/", createApi(createPlatformAdapter(runtime, runtime.store, [], {
      ttl: resolveThreadTtlPolicy(undefined, {}),
    })));
    const client = (user: string) => new Client({ apiUrl: "http://valida.test", apiKey: null,
      defaultHeaders: { "x-user": user },
      callerOptions: { maxRetries: 0,
        fetch: ((input: RequestInfo | URL, init?: RequestInit) =>
          app.fetch(new Request(input, init))) as typeof fetch } });

    const ordinary = await client("alice").threads.create();
    expect(await runtime.store.getThreadTtl(ordinary.thread_id)).toBeNull();
    const thread = await client("alice").threads.create({ ttl: 2 });
    const first = (await runtime.store.getThreadTtl(thread.thread_id))!;
    expect(first.strategy).toBe("delete");
    expect(new Date(first.expiresAt).getTime() - new Date(first.createdAt).getTime()).toBe(120_000);

    await client("alice").threads.update(thread.thread_id, { metadata: { label: "kept" } });
    expect((await runtime.store.getThreadTtl(thread.thread_id))?.expiresAt).toBe(first.expiresAt);
    await client("alice").threads.update(thread.thread_id, { ttl: 4 });
    const renewed = (await runtime.store.getThreadTtl(thread.thread_id))!;
    expect(new Date(renewed.expiresAt).getTime()).toBeGreaterThan(new Date(first.expiresAt).getTime());
    await expect(client("bob").threads.update(thread.thread_id, { ttl: 10 }))
      .rejects.toThrow("HTTP 403");
    expect((await runtime.store.getThreadTtl(thread.thread_id))?.expiresAt).toBe(renewed.expiresAt);

    const replaced = await client("alice").threads.create({ metadata: { preset: "short" } });
    expect((await runtime.store.getThreadTtl(replaced.thread_id))?.ttlMinutes).toBe(0.5);
    const keep = await app.request("/threads", { method: "POST",
      headers: { "x-user": "alice", "content-type": "application/json" },
      body: JSON.stringify({ ttl: { ttl: 5, strategy: "keep_latest" } }) });
    expect(keep.status).toBe(200);
    const keepId = (await keep.json() as { thread_id: string }).thread_id;
    expect((await runtime.store.getThreadTtl(keepId))?.strategy).toBe("keep_latest");

    const invalidId = crypto.randomUUID();
    await expect(client("alice").threads.create({ threadId: invalidId, ttl: 0 }))
      .rejects.toThrow("HTTP 422");
    expect(await runtime.store.getThread(invalidId)).toBeNull();

    await runtime.store.exec(sql`UPDATE thread_ttl SET expires_at = ${"2020-01-01T00:00:00.000Z"}
      WHERE thread_id = ${thread.thread_id}`);
    expect(await new ThreadPruner(runtime.store).sweepExpired()).toEqual({ deleted: 1, pruned: 0 });
    await expect(client("alice").threads.get(thread.thread_id)).rejects.toThrow("HTTP 404");
  } finally {
    await runtime.close();
  }
});

test("an explicit TTL config supplies a default only to newly created threads", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite", url: ":memory:" } });
  try {
    const policy = resolveThreadTtlPolicy({ ttl: { default_ttl: 3, strategy: "keep_latest" } }, {})!;
    const app = createApi(createPlatformAdapter(runtime, runtime.store, [], { ttl: policy }));
    const client = new Client({ apiUrl: "http://valida.test", apiKey: null,
      callerOptions: { maxRetries: 0,
        fetch: ((input: RequestInfo | URL, init?: RequestInit) =>
          app.fetch(new Request(input, init))) as typeof fetch } });
    const thread = await client.threads.create();
    expect(await runtime.store.getThreadTtl(thread.thread_id)).toMatchObject({
      ttlMinutes: 3, strategy: "keep_latest",
    });
    await client.threads.update(thread.thread_id, { metadata: { label: "updated" } });
    expect((await runtime.store.getThreadTtl(thread.thread_id))?.ttlMinutes).toBe(3);
  } finally {
    await runtime.close();
  }
});
