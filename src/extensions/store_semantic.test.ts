import { expect, test } from "bun:test";
import { sql } from "drizzle-orm";
import { createHash } from "node:crypto";
import postgres from "postgres";
import { createRuntime } from "../engine/index.ts";
import { loadStoreIndex } from "./store_config.ts";
import { createStoreExtension } from "./store.ts";

const context = { request: new Request("http://test") };
const config = { dims: 3, embed: "./examples/embeddings.ts:embedTexts", fields: ["text"] };

test("module-backed index ranks, filters, paginates, updates and deletes deterministically", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite", url: ":memory:" } });
  try {
    const index = await loadStoreIndex(config, process.cwd());
    const kv = await createStoreExtension(runtime.store, { index });
    await kv.put({ namespace: ["memory", "alice"], key: "apple", value: { text: "apple orchard", role: "fruit" } }, context);
    await kv.put({ namespace: ["memory", "alice"], key: "mixed", value: { text: "apple banana", role: "fruit" } }, context);
    await kv.put({ namespace: ["memory", "alice"], key: "coffee", value: { text: "coffee beans", role: "drink" } }, context);
    await kv.put({ namespace: ["memory", "bob"], key: "private", value: { text: "apple", role: "fruit" } }, context);

    const result = await kv.search({ namespace_prefix: ["memory", "alice"], query: "apple" }, context);
    expect((result.items as Array<{ key: string }>).map(item => item.key)).toEqual(["apple", "mixed", "coffee"]);
    const scores = (result.items as Array<{ score: number }>).map(item => item.score);
    expect(scores[0]).toBe(1);
    expect(scores[1]).toBeCloseTo(Math.SQRT1_2);
    expect(scores[2]).toBe(0);
    expect(result.total).toBe(3);
    const page = await kv.search({ namespace_prefix: ["memory", "alice"], query: "apple",
      filter: { role: "fruit" }, limit: 1, offset: 1 }, context);
    expect((page.items as Array<{ key: string }>).map(item => item.key)).toEqual(["mixed"]);
    expect(page.total).toBe(2);

    await kv.put({ namespace: ["memory", "alice"], key: "apple", value: { text: "banana", role: "fruit" } }, context);
    const changed = await kv.search({ namespace_prefix: ["memory", "alice"], query: "apple" }, context);
    expect((changed.items as Array<{ key: string }>)[0]?.key).toBe("mixed");
    expect((changed.items as Array<{ key: string; score: number }>).find(item => item.key === "apple")?.score).toBe(0);

    await kv.put({ namespace: ["memory", "alice"], key: "mixed",
      value: { text: "apple banana", role: "fruit" }, index: false }, context);
    const unindexed = await kv.search({ namespace_prefix: ["memory", "alice"], query: "apple" }, context);
    expect((unindexed.items as Array<{ key: string }>).some(item => item.key === "mixed")).toBe(false);
    expect(await kv.get(["memory", "alice"], "mixed", context)).not.toBeNull();

    await kv.delete(["memory", "alice"], "coffee", context);
    expect((await runtime.store.rows(sql`SELECT * FROM valida_store_embeddings WHERE item_key = ${"coffee"}`)).length).toBe(0);
    await runtime.store.exec(sql`UPDATE valida_store_items SET expires_at = ${"2000-01-01T00:00:00.000Z"}
      WHERE item_key = ${"apple"} AND namespace = ${JSON.stringify(["memory", "alice"])}`);
    expect((await kv.search({ namespace_prefix: ["memory", "alice"], query: "apple" }, context)).total).toBe(0);
  } finally {
    await runtime.close();
  }
});

test("index checks vector dimensions and norm before writing", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite", url: ":memory:" } });
  try {
    const malformed = await createStoreExtension(runtime.store, { index: { dims: 2, embed: () => [[1]] } });
    await expect(malformed.put({ namespace: ["test"], key: "bad", value: { text: "apple" } }, context))
      .rejects.toThrow("2-dimensional");
    expect(await malformed.get(["test"], "bad", context)).toBeNull();
    const zero = await createStoreExtension(runtime.store, { index: { dims: 2, embed: () => [[0, 0]] } });
    await expect(zero.put({ namespace: ["test"], key: "zero", value: { text: "apple" } }, context))
      .rejects.toThrow("zero or invalid vector norm");
    expect(await zero.get(["test"], "zero", context)).toBeNull();
  } finally {
    await runtime.close();
  }
});

test("index reads nested fields and per-item field overrides", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite", url: ":memory:" } });
  try {
    const index = await loadStoreIndex(config, process.cwd());
    const kv = await createStoreExtension(runtime.store, { index });
    await kv.put({ namespace: ["nested"], key: "bio",
      value: { text: "coffee", profile: { bio: "apple orchard" } }, index: ["profile.bio"] }, context);
    await kv.put({ namespace: ["nested"], key: "missing",
      value: { profile: { bio: "apple" } } }, context);
    const found = await kv.search({ namespace_prefix: ["nested"], query: "apple" }, context);
    expect((found.items as Array<{ key: string; score: number }>).map(item => [item.key, item.score]))
      .toEqual([["bio", 1]]);
    expect(await kv.get(["nested"], "missing", context)).not.toBeNull();
  } finally {
    await runtime.close();
  }
});

if (process.env.TEST_POSTGRES_URL) test("PostgreSQL uses the same cosine ranking and TTL rules", async () => {
  const runtime = await createRuntime({ db: { dialect: "postgres", url: process.env.TEST_POSTGRES_URL! } });
  const ns = ["semantic-test", crypto.randomUUID()];
  try {
    const index = await loadStoreIndex(config, process.cwd());
    const kv = await createStoreExtension(runtime.store, { index });
    await kv.put({ namespace: ns, key: "one", value: { text: "apple", role: "fruit" } }, context);
    await kv.put({ namespace: ns, key: "two", value: { text: "apple banana", role: "fruit" } }, context);
    const found = await kv.search({ namespace_prefix: ns, query: "apple", filter: { role: "fruit" } }, context);
    expect((found.items as Array<{ key: string }>).map(item => item.key)).toEqual(["one", "two"]);
    expect(Number((found.items as Array<{ score: number }>)[0]?.score)).toBeCloseTo(1);
    const page = await kv.search({ namespace_prefix: ns, query: "apple", limit: 1, offset: 1 }, context);
    expect((page.items as Array<{ key: string }>)[0]?.key).toBe("two");
    expect(page.total).toBe(2);
    const native = await runtime.store.rows<{ dims: number; embedding: string }>(sql`
      SELECT dims, embedding::text AS embedding FROM valida_store_vectors
      WHERE namespace = ${JSON.stringify(ns)} ORDER BY item_key`);
    expect(native).toHaveLength(2);
    expect(native[0]).toMatchObject({ dims: 3, embedding: "[1,0,0]" });
    const indexes = await runtime.store.rows<{ indexdef: string }>(sql`
      SELECT indexdef FROM pg_indexes WHERE indexname = ${"valida_store_vectors_hnsw_3"}`);
    expect(indexes[0]?.indexdef).toContain("USING hnsw");
    expect(indexes[0]?.indexdef).toContain("vector_cosine_ops");

    const connection = postgres(process.env.TEST_POSTGRES_URL!, { max: 1 });
    try {
      const plan = await connection.begin(async tx => {
        await tx`SET LOCAL enable_seqscan = off`;
        return tx`EXPLAIN SELECT i.item_key FROM valida_store_vectors v
          JOIN valida_store_items i ON i.namespace = v.namespace AND i.item_key = v.item_key
          WHERE v.dims = 3 AND v.source_hash = md5(i.item_value)
          ORDER BY v.embedding::vector(3) <=> '[1,0,0]'::vector(3) LIMIT 2`;
      });
      expect(plan.map(row => String(row["QUERY PLAN"])).join("\n")).toContain("valida_store_vectors_hnsw_3");
    } finally {
      await connection.end();
    }

    await kv.put({ namespace: ns, key: "one", value: { text: "banana", role: "fruit" } }, context);
    expect((await kv.search({ namespace_prefix: ns, query: "apple" }, context)).items)
      .toMatchObject([{ key: "two" }, { key: "one", score: 0 }]);
    await runtime.store.exec(sql`UPDATE valida_store_items SET expires_at = ${"2000-01-01T00:00:00.000Z"}
      WHERE namespace = ${JSON.stringify(ns)} AND item_key = ${"one"}`);
    expect((await kv.search({ namespace_prefix: ns, query: "apple" }, context)).total).toBe(1);
    await kv.delete(ns, "two", context);
    expect((await kv.search({ namespace_prefix: ns, query: "apple" }, context)).total).toBe(0);

    await Promise.all([
      kv.put({ namespace: ns, key: "race", value: { text: "apple" } }, context),
      kv.put({ namespace: ns, key: "race", value: { text: "banana" } }, context),
    ]);
    const current = await kv.get(ns, "race", context);
    const text = (current?.value as { text: string }).text;
    const raced = await kv.search({ namespace_prefix: ns, query: text }, context);
    expect((raced.items as Array<{ key: string; score: number }>)[0]).toMatchObject({ key: "race", score: 1 });
  } finally {
    await runtime.store.exec(sql`DELETE FROM valida_store_vectors WHERE namespace = ${JSON.stringify(ns)}`);
    await runtime.store.exec(sql`DELETE FROM valida_store_embeddings WHERE namespace = ${JSON.stringify(ns)}`);
    await runtime.store.exec(sql`DELETE FROM valida_store_items WHERE namespace = ${JSON.stringify(ns)}`);
    await runtime.close();
  }
});

if (process.env.TEST_POSTGRES_URL) test("PostgreSQL imports valid legacy JSON embeddings into native vectors", async () => {
  const runtime = await createRuntime({ db: { dialect: "postgres", url: process.env.TEST_POSTGRES_URL! } });
  const ns = JSON.stringify(["legacy", crypto.randomUUID()]);
  try {
    const index = await loadStoreIndex(config, process.cwd());
    await createStoreExtension(runtime.store);
    const source = JSON.stringify({ text: "apple" });
    const stamp = new Date().toISOString();
    await runtime.store.exec(sql`INSERT INTO valida_store_items
      (namespace, item_key, item_value, created_at, updated_at, expires_at)
      VALUES (${ns}, ${"old"}, ${source}, ${stamp}, ${stamp}, ${null})`);
    await runtime.store.exec(sql`INSERT INTO valida_store_embeddings
      (namespace, item_key, source_hash, vectors)
      VALUES (${ns}, ${"old"}, ${createHash("sha256").update(source).digest("hex")},
        ${JSON.stringify([{ field: "text", vector: [1, 0, 0] }])})`);
    const kv = await createStoreExtension(runtime.store, { index });
    const found = await kv.search({ namespace_prefix: JSON.parse(ns) as string[], query: "apple" }, context);
    expect((found.items as Array<{ key: string; score: number }>)[0]).toMatchObject({ key: "old", score: 1 });
    const rows = await runtime.store.rows(sql`SELECT * FROM valida_store_vectors WHERE namespace = ${ns}`);
    expect(rows).toHaveLength(1);
  } finally {
    await runtime.store.exec(sql`DELETE FROM valida_store_vectors WHERE namespace = ${ns}`);
    await runtime.store.exec(sql`DELETE FROM valida_store_embeddings WHERE namespace = ${ns}`);
    await runtime.store.exec(sql`DELETE FROM valida_store_items WHERE namespace = ${ns}`);
    await runtime.close();
  }
});

if (process.env.TEST_POSTGRES_URL) test("PostgreSQL creates a halfvec HNSW index for 3072 dimensions", async () => {
  const runtime = await createRuntime({ db: { dialect: "postgres", url: process.env.TEST_POSTGRES_URL! } });
  const ns = ["wide", crypto.randomUUID()];
  const wide = (texts: string[]) => texts.map(() => [1, ...Array<number>(3071).fill(0)]);
  try {
    const kv = await createStoreExtension(runtime.store, { index: { dims: 3072, embed: wide, fields: ["text"] } });
    await kv.put({ namespace: ns, key: "one", value: { text: "apple" } }, context);
    expect((await kv.search({ namespace_prefix: ns, query: "apple" }, context)).items)
      .toMatchObject([{ key: "one", score: 1 }]);
    const index = await runtime.store.rows<{ indexdef: string }>(sql`
      SELECT indexdef FROM pg_indexes WHERE indexname = ${"valida_store_vectors_hnsw_3072"}`);
    expect(index[0]?.indexdef).toContain("halfvec_cosine_ops");
  } finally {
    await runtime.store.exec(sql`DELETE FROM valida_store_vectors WHERE namespace = ${JSON.stringify(ns)}`);
    await runtime.store.exec(sql`DELETE FROM valida_store_items WHERE namespace = ${JSON.stringify(ns)}`);
    await runtime.close();
  }
});
