import { sql } from "drizzle-orm";
import { createHash } from "node:crypto";
import type { Store } from "../db/index.ts";
import type { ApiRequestContext, JsonRecord, PlatformAdapter } from "../api/types.ts";
import { ApiError } from "../api/types.ts";
import { createRelation } from "./ddl.ts";
import { matchesAuthorizationFilter } from "../authz.ts";

type Row = Record<string, unknown>;
type StoreBackend = NonNullable<PlatformAdapter["store"]>;

export type EmbeddingProvider = (texts: string[]) => number[][] | Promise<number[][]>;
export interface StoreIndexConfig {
  dims: number;
  embed: EmbeddingProvider;
  fields?: string[];
}
export interface StoreOptions { index?: StoreIndexConfig }

const now = () => new Date().toISOString();
const object = (value: unknown): JsonRecord =>
  value !== null && typeof value === "object" && !Array.isArray(value) ? value as JsonRecord : {};
const integer = (value: unknown, fallback: number, max = 1000) => {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed >= 0 ? Math.min(parsed, max) : fallback;
};
const namespace = (value: unknown, field = "namespace"): string[] => {
  if (!Array.isArray(value) || !value.every((part) => typeof part === "string" && part.length > 0 && !part.includes("."))) {
    throw new ApiError(422, `${field} must be an array of non-empty strings without periods`);
  }
  return value;
};
const decode = (value: unknown): JsonRecord => {
  try { return object(JSON.parse(String(value))); } catch { return {}; }
};
const item = (row: Row): JsonRecord => ({
  namespace: JSON.parse(String(row.namespace)) as string[],
  key: String(row.item_key),
  value: decode(row.item_value),
  created_at: String(row.created_at),
  updated_at: String(row.updated_at),
});
const active = (row: Row, stamp: string) => row.expires_at == null || String(row.expires_at) > stamp;
const startsWith = (parts: string[], prefix: string[]) => prefix.every((part, index) => parts[index] === part);
const endsWith = (parts: string[], suffix: string[]) => suffix.every((part, index) => parts[parts.length - suffix.length + index] === part);
const hash = (value: string) => createHash("sha256").update(value).digest("hex");

function indexFields(fields: unknown): string[] {
  if (!Array.isArray(fields) || !fields.every(field =>
    typeof field === "string" && (field === "$" || (field.length > 0 && field.split(".").every(Boolean))))) {
    throw new ApiError(422, "index fields must be non-empty JSON paths");
  }
  return fields;
}

function fieldText(value: JsonRecord, field: string): string | null {
  let selected: unknown = value;
  if (field !== "$") {
    for (const part of field.split(".")) {
      if (selected === null || typeof selected !== "object" || !(part in selected)) return null;
      selected = (selected as Record<string, unknown>)[part];
    }
  }
  if (selected === null || selected === undefined) return null;
  return typeof selected === "string" ? selected : JSON.stringify(selected);
}

function unitVector(vector: unknown, dims: number): number[] {
  if (!Array.isArray(vector) || vector.length !== dims ||
    !vector.every(value => typeof value === "number" && Number.isFinite(value))) {
    throw new Error(`Embedding provider must return finite ${dims}-dimensional vectors`);
  }
  const norm = Math.hypot(...vector);
  if (!Number.isFinite(norm) || norm === 0) throw new Error("Embedding provider returned a zero or invalid vector norm");
  return vector.map(value => value / norm);
}

async function embed(index: StoreIndexConfig, texts: string[]): Promise<number[][]> {
  const output = await index.embed(texts);
  if (!Array.isArray(output) || output.length !== texts.length) {
    throw new Error(`Embedding provider must return ${texts.length} vectors`);
  }
  return output.map(vector => unitVector(vector, index.dims));
}

function vectorScore(left: number[], right: number[]): number {
  return Math.max(-1, Math.min(1, left.reduce((sum, value, index) => sum + value * right[index]!, 0)));
}

/** Durable JSON store with optional portable cosine-similarity search. */
export async function createStoreExtension(store: Store, options: StoreOptions = {}): Promise<StoreBackend> {
  const index = options.index;
  if (index && (!Number.isSafeInteger(index.dims) || index.dims <= 0 || typeof index.embed !== "function")) {
    throw new Error("Store index requires positive integer dims and an embedding function");
  }
  const fields = index ? indexFields(index.fields ?? ["$"]) : [];
  await createRelation(store, "valida_store_items", `CREATE TABLE IF NOT EXISTS valida_store_items (
    namespace TEXT NOT NULL,
    item_key TEXT NOT NULL,
    item_value TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    expires_at TEXT,
    PRIMARY KEY (namespace, item_key)
  )`);
  await createRelation(store, "valida_store_items_expires",
    "CREATE INDEX IF NOT EXISTS valida_store_items_expires ON valida_store_items (expires_at)");
  await createRelation(store, "valida_store_embeddings", `CREATE TABLE IF NOT EXISTS valida_store_embeddings (
    namespace TEXT NOT NULL,
    item_key TEXT NOT NULL,
    source_hash TEXT NOT NULL,
    vectors TEXT NOT NULL,
    PRIMARY KEY (namespace, item_key)
  )`);

  return {
    async put(payload: JsonRecord, _context?: ApiRequestContext): Promise<void> {
      const ns = namespace(payload.namespace);
      const key = payload.key;
      if (typeof key !== "string" || key.length === 0) throw new ApiError(422, "key is required");
      if (!payload.value || typeof payload.value !== "object" || Array.isArray(payload.value)) {
        throw new ApiError(422, "value must be a JSON object");
      }
      const ttl = payload.ttl == null ? null : Number(payload.ttl);
      if (ttl !== null && (!Number.isFinite(ttl) || ttl <= 0)) throw new ApiError(422, "ttl must be positive minutes");
      const stamp = now();
      const expiresAt = ttl === null ? null : new Date(Date.now() + ttl * 60_000).toISOString();
      const source = JSON.stringify(payload.value);
      let vectors: Array<{ field: string; vector: number[] }> = [];
      if (index && payload.index !== false) {
        const selectedFields = payload.index == null || payload.index === true ? fields : indexFields(payload.index);
        const entries = selectedFields.map(field => ({ field, text: fieldText(payload.value as JsonRecord, field) }))
          .filter((entry): entry is { field: string; text: string } => entry.text !== null);
        if (entries.length) {
          const embeddings = await embed(index, entries.map(entry => entry.text));
          vectors = entries.map((entry, i) => ({ field: entry.field, vector: embeddings[i]! }));
        }
      }
      await store.exec(sql`INSERT INTO valida_store_items
        (namespace, item_key, item_value, created_at, updated_at, expires_at)
        VALUES (${JSON.stringify(ns)}, ${key}, ${source}, ${stamp}, ${stamp}, ${expiresAt})
        ON CONFLICT (namespace, item_key) DO UPDATE SET
        item_value = excluded.item_value,
        updated_at = excluded.updated_at,
        expires_at = excluded.expires_at`);
      if (vectors.length) {
        const name = JSON.stringify(ns);
        await store.exec(sql`INSERT INTO valida_store_embeddings (namespace, item_key, source_hash, vectors)
          SELECT namespace, item_key, ${hash(source)}, ${JSON.stringify(vectors)}
          FROM valida_store_items WHERE namespace = ${name} AND item_key = ${key} AND item_value = ${source}
          ON CONFLICT (namespace, item_key) DO UPDATE SET
          source_hash = excluded.source_hash, vectors = excluded.vectors`);
      } else {
        await store.exec(sql`DELETE FROM valida_store_embeddings
          WHERE namespace = ${JSON.stringify(ns)} AND item_key = ${key}
          AND EXISTS (SELECT 1 FROM valida_store_items WHERE namespace = ${JSON.stringify(ns)}
            AND item_key = ${key} AND item_value = ${source})`);
      }
    },
    async get(ns: string[], key: string, _context?: ApiRequestContext): Promise<JsonRecord | null> {
      const name = JSON.stringify(namespace(ns));
      const rows = await store.rows<Row>(sql`SELECT * FROM valida_store_items
        WHERE namespace = ${name} AND item_key = ${key} LIMIT 1`);
      const row = rows[0];
      if (!row || !active(row, now())) return null;
      const result = item(row);
      return matchesAuthorizationFilter("store", result) ? result : null;
    },
    async delete(ns: string[], key: string, _context?: ApiRequestContext): Promise<void> {
      const name = JSON.stringify(namespace(ns));
      const row = (await store.rows<Row>(sql`SELECT * FROM valida_store_items
        WHERE namespace = ${name} AND item_key = ${key} LIMIT 1`))[0];
      if (row && !matchesAuthorizationFilter("store", item(row))) throw new ApiError(403, "Store item access denied");
      await store.exec(sql`DELETE FROM valida_store_items
        WHERE namespace = ${name} AND item_key = ${key}`);
      await store.exec(sql`DELETE FROM valida_store_embeddings
        WHERE namespace = ${name} AND item_key = ${key}`);
    },
    async search(payload: JsonRecord, _context?: ApiRequestContext): Promise<JsonRecord> {
      const query = payload.query;
      if (query != null && typeof query !== "string") throw new ApiError(422, "query must be a string");
      if (query && !index) {
        throw new ApiError(501, "Semantic search requires a configured embedding index");
      }
      const prefix = namespace(payload.namespace_prefix ?? [], "namespace_prefix");
      const filter = object(payload.filter);
      const limit = integer(payload.limit, 10);
      const offset = integer(payload.offset, 0, Number.MAX_SAFE_INTEGER);
      const rows = query ? await store.rows<Row>(sql`SELECT i.*, e.source_hash AS embedding_hash,
          e.vectors AS embedding_vectors FROM valida_store_items i
          LEFT JOIN valida_store_embeddings e ON i.namespace = e.namespace AND i.item_key = e.item_key
          ORDER BY i.created_at DESC, i.item_key`)
        : await store.rows<Row>(sql`SELECT * FROM valida_store_items ORDER BY created_at DESC, item_key`);
      const stamp = now();
      let matches = rows.filter((row) => {
        if (!active(row, stamp)) return false;
        const candidate = JSON.parse(String(row.namespace)) as string[];
        if (!startsWith(candidate, prefix)) return false;
        const value = decode(row.item_value);
        return matchesAuthorizationFilter("store", item(row)) &&
          Object.entries(filter).every(([key, expected]) => JSON.stringify(value[key]) === JSON.stringify(expected));
      });
      if (query && index) {
        const queryVector = (await embed(index, [query]))[0]!;
        matches = matches.filter(row => row.embedding_hash === hash(String(row.item_value)));
        const scored = matches.flatMap(row => {
          let vectors: Array<{ field: string; vector: number[] }>;
          try { vectors = JSON.parse(String(row.embedding_vectors)) as typeof vectors; }
          catch { return []; }
          const scores = vectors.filter(entry => Array.isArray(entry.vector) && entry.vector.length === index.dims)
            .map(entry => vectorScore(queryVector, entry.vector));
          return scores.length ? [{ row, score: Math.max(...scores) }] : [];
        }).sort((a, b) => b.score - a.score);
        return { items: scored.slice(offset, offset + limit).map(({ row, score }) => ({ ...item(row), score })),
          total: scored.length, limit, offset };
      }
      return { items: matches.slice(offset, offset + limit).map(item), total: matches.length, limit, offset };
    },
    async namespaces(payload: JsonRecord, _context?: ApiRequestContext): Promise<JsonRecord> {
      const prefix = payload.prefix == null ? [] : namespace(payload.prefix, "prefix");
      const suffix = payload.suffix == null ? [] : namespace(payload.suffix, "suffix");
      const maxDepth = payload.max_depth == null ? null : integer(payload.max_depth, 0, 100);
      if (maxDepth === 0) throw new ApiError(422, "max_depth must be positive");
      const limit = integer(payload.limit, 100);
      const offset = integer(payload.offset, 0, Number.MAX_SAFE_INTEGER);
      const rows = await store.rows<Row>(sql`SELECT * FROM valida_store_items`);
      const stamp = now();
      const names = new Map<string, string[]>();
      for (const row of rows) {
        if (!active(row, stamp)) continue;
        if (!matchesAuthorizationFilter("store", item(row))) continue;
        const parts = JSON.parse(String(row.namespace)) as string[];
        if (!startsWith(parts, prefix) || !endsWith(parts, suffix)) continue;
        const trimmed = maxDepth === null ? parts : parts.slice(0, maxDepth);
        names.set(JSON.stringify(trimmed), trimmed);
      }
      return { namespaces: [...names.values()].sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b))).slice(offset, offset + limit) };
    },
  };
}
