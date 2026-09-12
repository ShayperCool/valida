import { sql } from "drizzle-orm";
import type { Store } from "../db/index.ts";

const expectedRaceCodes = new Set(["23505", "42P07", "42710"]);

/**
 * PostgreSQL's IF NOT EXISTS check can race with a concurrent CREATE on a
 * fresh database. Only suppress a duplicate-object error after the named
 * relation is visible to this connection. Every other DDL failure propagates.
 */
export async function createRelation(store: Store, name: string, ddl: string): Promise<void> {
  try {
    await store.exec(sql.raw(ddl));
  } catch (cause) {
    if (store.dialect !== "postgres") throw cause;
    const error = cause as { code?: unknown; cause?: { code?: unknown } };
    const code = typeof error.code === "string" ? error.code : error.cause?.code;
    if (typeof code !== "string" || !expectedRaceCodes.has(code)) throw cause;
    const rows = await store.rows<{ exists: boolean }>(sql`SELECT to_regclass(${name}) IS NOT NULL AS exists`);
    if (rows[0]?.exists !== true) throw cause;
  }
}
