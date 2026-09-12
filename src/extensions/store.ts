import { sql } from "drizzle-orm";
import type { Store } from "../db/index.ts";
import type { ApiRequestContext, JsonRecord, PlatformAdapter } from "../api/types.ts";
import { ApiError } from "../api/types.ts";
import { createRelation } from "./ddl.ts";

type Row = Record<string, unknown>;
type StoreBackend = NonNullable<PlatformAdapter["store"]>;

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

/** Durable exact-key JSON store. Semantic vector search needs a separately configured index. */
export async function createStoreExtension(store: Store): Promise<StoreBackend> {
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
      await store.exec(sql`INSERT INTO valida_store_items
        (namespace, item_key, item_value, created_at, updated_at, expires_at)
        VALUES (${JSON.stringify(ns)}, ${key}, ${JSON.stringify(payload.value)}, ${stamp}, ${stamp}, ${expiresAt})
        ON CONFLICT (namespace, item_key) DO UPDATE SET
        item_value = excluded.item_value,
        updated_at = excluded.updated_at,
        expires_at = excluded.expires_at`);
    },
    async get(ns: string[], key: string, _context?: ApiRequestContext): Promise<JsonRecord | null> {
      const name = JSON.stringify(namespace(ns));
      const rows = await store.rows<Row>(sql`SELECT * FROM valida_store_items
        WHERE namespace = ${name} AND item_key = ${key} LIMIT 1`);
      const row = rows[0];
      if (!row || !active(row, now())) return null;
      return item(row);
    },
    async delete(ns: string[], key: string, _context?: ApiRequestContext): Promise<void> {
      await store.exec(sql`DELETE FROM valida_store_items
        WHERE namespace = ${JSON.stringify(namespace(ns))} AND item_key = ${key}`);
    },
    async search(payload: JsonRecord, _context?: ApiRequestContext): Promise<JsonRecord> {
      if (typeof payload.query === "string" && payload.query.length > 0) {
        throw new ApiError(501, "Semantic search requires a configured embedding index");
      }
      const prefix = namespace(payload.namespace_prefix ?? [], "namespace_prefix");
      const filter = object(payload.filter);
      const limit = integer(payload.limit, 10);
      const offset = integer(payload.offset, 0, Number.MAX_SAFE_INTEGER);
      const rows = await store.rows<Row>(sql`SELECT * FROM valida_store_items ORDER BY created_at DESC, item_key`);
      const stamp = now();
      const matches = rows.filter((row) => {
        if (!active(row, stamp)) return false;
        const candidate = JSON.parse(String(row.namespace)) as string[];
        if (!startsWith(candidate, prefix)) return false;
        const value = decode(row.item_value);
        return Object.entries(filter).every(([key, expected]) => JSON.stringify(value[key]) === JSON.stringify(expected));
      });
      return { items: matches.slice(offset, offset + limit).map(item), total: matches.length, limit, offset };
    },
    async namespaces(payload: JsonRecord, _context?: ApiRequestContext): Promise<JsonRecord> {
      const prefix = payload.prefix == null ? [] : namespace(payload.prefix, "prefix");
      const suffix = payload.suffix == null ? [] : namespace(payload.suffix, "suffix");
      const maxDepth = payload.max_depth == null ? null : integer(payload.max_depth, 0, 100);
      if (maxDepth === 0) throw new ApiError(422, "max_depth must be positive");
      const limit = integer(payload.limit, 100);
      const offset = integer(payload.offset, 0, Number.MAX_SAFE_INTEGER);
      const rows = await store.rows<Row>(sql`SELECT namespace, expires_at FROM valida_store_items`);
      const stamp = now();
      const names = new Map<string, string[]>();
      for (const row of rows) {
        if (!active(row, stamp)) continue;
        const parts = JSON.parse(String(row.namespace)) as string[];
        if (!startsWith(parts, prefix) || !endsWith(parts, suffix)) continue;
        const trimmed = maxDepth === null ? parts : parts.slice(0, maxDepth);
        names.set(JSON.stringify(trimmed), trimmed);
      }
      return { namespaces: [...names.values()].sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b))).slice(offset, offset + limit) };
    },
  };
}
