import { expect, test } from "bun:test";
import { createStore } from "./index.ts";

test("thread TTL is stored atomically and metadata-only updates preserve expiry", async () => {
  const store = await createStore({ dialect: "sqlite", url: ":memory:" });
  try {
    const without = await store.createThread();
    expect(await store.getThreadTtl(without.id)).toBeNull();

    const thread = await store.createThread({ ttl: { ttlMinutes: 2, strategy: "delete" } });
    const first = (await store.getThreadTtl(thread.id))!;
    expect(first.strategy).toBe("delete");
    expect(first.ttlMinutes).toBe(2);
    expect(new Date(first.expiresAt).getTime() - new Date(first.createdAt).getTime()).toBe(120_000);

    await store.updateThread(thread.id, { metadata: { label: "kept" } });
    expect((await store.getThreadTtl(thread.id))?.expiresAt).toBe(first.expiresAt);
    await store.updateThread(thread.id, { ttl: { ttlMinutes: 4, strategy: "keep_latest" } });
    const renewed = (await store.getThreadTtl(thread.id))!;
    expect(renewed.strategy).toBe("keep_latest");
    expect(renewed.createdAt).toBe(first.createdAt);
    expect(new Date(renewed.expiresAt).getTime()).toBeGreaterThan(new Date(first.expiresAt).getTime());

    await store.deleteThread(thread.id);
    expect(await store.getThreadTtl(thread.id)).toBeNull();
    expect(await store.getThread(thread.id)).toBeNull();

    const invalidId = crypto.randomUUID();
    await expect(store.createThread({ id: invalidId, ttl: { ttlMinutes: 0, strategy: "delete" } }))
      .rejects.toThrow();
    expect(await store.getThread(invalidId)).toBeNull();
  } finally {
    await store.close();
  }
});
