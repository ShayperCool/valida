import { createHash } from "node:crypto";
import { sql, type SQL } from "drizzle-orm";
import type { Store } from "../db/index.ts";
import type { JsonRecord } from "../api/types.ts";
import { currentAuthorization } from "../auth.ts";
import { matchesAuthorizationFilter } from "../authz.ts";
import { createRelation } from "./ddl.ts";

type Row = Record<string, unknown>;
export interface IndexedVector { field: string; vector: number[] }
interface SearchOptions {
  prefix: string[]; filter: JsonRecord; queryVector: number[];
  dims: number; limit: number; offset: number; stamp: string;
}

const md5 = (value: string) => createHash("md5").update(value).digest("hex");
const sha256 = (value: string) => createHash("sha256").update(value).digest("hex");
const dataType = (dims: number) => dims <= 2000 ? "vector" : "halfvec";

async function ensureExtension(store: Store): Promise<void> {
  try {
    await store.exec(sql`CREATE EXTENSION IF NOT EXISTS vector`);
  } catch (cause) {
    const error = cause as { code?: unknown; cause?: { code?: unknown } };
    const code = error.code ?? error.cause?.code;
    if (code !== "23505" && code !== "42710") throw cause;
    const rows = await store.rows<{ installed: boolean }>(sql`
      SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'vector') AS installed`);
    if (!rows[0]?.installed) throw cause;
  }
}

/** A partial HNSW index is needed because model dimensions are configurable. */
export async function preparePgvectorStore(store: Store, dims: number): Promise<void> {
  if (!Number.isSafeInteger(dims) || dims <= 0 || dims > 4000) {
    throw new Error("PostgreSQL pgvector HNSW supports 1-4000 dimensions (halfvec above 2000)");
  }
  await ensureExtension(store);
  await createRelation(store, "valida_store_vectors", `CREATE TABLE IF NOT EXISTS valida_store_vectors (
    namespace TEXT NOT NULL,
    item_key TEXT NOT NULL,
    field TEXT NOT NULL,
    source_hash TEXT NOT NULL,
    dims INTEGER NOT NULL,
    embedding vector NOT NULL,
    PRIMARY KEY (namespace, item_key, field)
  )`);
  const type = dataType(dims);
  const name = `valida_store_vectors_hnsw_${dims}`;
  await createRelation(store, name, `CREATE INDEX IF NOT EXISTS ${name}
    ON valida_store_vectors USING hnsw ((embedding::${type}(${dims})) ${type}_cosine_ops)
    WHERE dims = ${dims}`);
  await backfillLegacyVectors(store, dims);
}

async function backfillLegacyVectors(store: Store, dims: number): Promise<void> {
  const rows = await store.rows<Row>(sql`SELECT e.namespace, e.item_key, e.source_hash, e.vectors,
      i.item_value FROM valida_store_embeddings e
      JOIN valida_store_items i ON i.namespace = e.namespace AND i.item_key = e.item_key
      WHERE NOT EXISTS (SELECT 1 FROM valida_store_vectors v
        WHERE v.namespace = e.namespace AND v.item_key = e.item_key)`);
  for (const row of rows) {
    const source = String(row.item_value);
    if (sha256(source) !== row.source_hash) continue;
    let entries: IndexedVector[];
    try { entries = JSON.parse(String(row.vectors)) as IndexedVector[]; }
    catch { continue; }
    if (!Array.isArray(entries)) continue;
    const vectors = entries.filter(entry => typeof entry.field === "string" &&
      Array.isArray(entry.vector) && entry.vector.length === dims &&
      entry.vector.every(value => typeof value === "number" && Number.isFinite(value))).slice(0, 64);
    if (vectors.length) await putPgVectors(store, String(row.namespace), String(row.item_key), source, vectors, dims);
  }
}

/** Statements run with the item upsert so concurrent writes cannot mix vectors. */
export function pgVectorWriteStatements(
  namespace: string, key: string, source: string,
  vectors: IndexedVector[], dims: number,
): SQL[] {
  const statements: SQL[] = [sql`DELETE FROM valida_store_vectors WHERE namespace = ${namespace} AND item_key = ${key}
    AND EXISTS (SELECT 1 FROM valida_store_items WHERE namespace = ${namespace}
      AND item_key = ${key} AND item_value = ${source})`];
  for (const entry of vectors) {
    statements.push(sql`INSERT INTO valida_store_vectors
      (namespace, item_key, field, source_hash, dims, embedding)
      SELECT namespace, item_key, ${entry.field}, ${md5(source)}, ${dims},
        ${JSON.stringify(entry.vector)}::vector FROM valida_store_items
      WHERE namespace = ${namespace} AND item_key = ${key} AND item_value = ${source}
      ON CONFLICT (namespace, item_key, field) DO UPDATE SET
        source_hash = excluded.source_hash, dims = excluded.dims, embedding = excluded.embedding`);
  }
  return statements;
}

/** Import pre-pgvector JSON embeddings without calling the model again. */
export async function putPgVectors(
  store: Store, namespace: string, key: string, source: string,
  vectors: IndexedVector[], dims: number,
): Promise<void> {
  await store.transaction(pgVectorWriteStatements(namespace, key, source, vectors, dims));
}

export async function deletePgVectors(store: Store, namespace: string, key: string): Promise<void> {
  await store.exec(sql`DELETE FROM valida_store_vectors WHERE namespace = ${namespace} AND item_key = ${key}`);
}

const item = (row: Row): JsonRecord => ({
  namespace: JSON.parse(String(row.namespace)) as string[],
  key: String(row.item_key),
  value: JSON.parse(String(row.item_value)) as JsonRecord,
  created_at: String(row.created_at),
  updated_at: String(row.updated_at),
});

/** Rank with pgvector in PostgreSQL; HNSW serves the bounded top-K query. */
export async function searchPgVectors(store: Store, options: SearchOptions): Promise<JsonRecord> {
  const { prefix, filter, queryVector, dims, limit, offset, stamp } = options;
  const type = dataType(dims);
  const cast = sql.raw(`${type}(${dims})`);
  const distance = sql`v.embedding::${cast} <=> ${JSON.stringify(queryVector)}::${cast}`;
  const conditions: SQL[] = [
    sql`v.dims = ${sql.raw(String(dims))}`,
    sql`v.source_hash = md5(i.item_value)`,
    sql`(i.expires_at IS NULL OR i.expires_at > ${stamp})`,
  ];
  for (const [position, part] of prefix.entries()) {
    conditions.push(sql`i.namespace::jsonb -> ${position}::integer = ${JSON.stringify(part)}::jsonb`);
  }
  for (const [key, value] of Object.entries(filter)) {
    conditions.push(sql`i.item_value::jsonb -> ${key} = ${JSON.stringify(value)}::jsonb`);
  }
  const where = sql.join(conditions, sql` AND `);
  const authorized = currentAuthorization.getStore();
  const needsAuthFilter = authorized?.resource === "store" && !!authorized.filter;
  let total: number | null = null;
  if (!needsAuthFilter) {
    const count = await store.rows<{ total: number | string }>(sql`
      SELECT COUNT(DISTINCT (v.namespace, v.item_key)) AS total
      FROM valida_store_vectors v JOIN valida_store_items i
        ON i.namespace = v.namespace AND i.item_key = v.item_key
      WHERE ${where}`);
    total = Number(count[0]?.total ?? 0);
    if (limit === 0 || offset >= total) return { items: [], total, limit, offset };
  }
  // Every put accepts at most 64 fields, so K*64 vector hits contain the top K
  // distinct items. Authorization filters need every hit for an exact total.
  const fetchRows = (bounded: boolean) => store.rows<Row>(sql`SELECT i.*, 1 - (${distance}) AS score
    FROM valida_store_vectors v JOIN valida_store_items i
      ON i.namespace = v.namespace AND i.item_key = v.item_key
    WHERE ${where} ORDER BY ${distance}
    ${bounded ? sql`LIMIT ${Math.min(Number.MAX_SAFE_INTEGER, (offset + limit) * 64)}` : sql.empty()}`);
  const uniqueItems = (rows: Row[]) => {
    const ranked = new Map<string, JsonRecord>();
    for (const row of rows) {
      const value = item(row);
      if (!matchesAuthorizationFilter("store", value)) continue;
      const id = JSON.stringify([value.namespace, value.key]);
      if (ranked.has(id)) continue;
      ranked.set(id, { ...value, score: Math.max(-1, Math.min(1, Number(row.score))) });
    }
    return [...ranked.values()].sort((a, b) =>
      Number(b.score) - Number(a.score) ||
      String(b.created_at).localeCompare(String(a.created_at)) || String(a.key).localeCompare(String(b.key)));
  };
  let items = uniqueItems(await fetchRows(!needsAuthFilter));
  // pgvector can return fewer than LIMIT hits after restrictive joins or filters.
  // An exact SQL distance scan fills the page if that happens.
  if (!needsAuthFilter && items.length < Math.min(total!, offset + limit)) {
    items = uniqueItems(await fetchRows(false));
  }
  return { items: items.slice(offset, offset + limit), total: total ?? items.length, limit, offset };
}
