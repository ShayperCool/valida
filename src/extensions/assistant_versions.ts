import { sql } from "drizzle-orm";
import type { AssistantRecord, Store } from "../db/index.ts";
import type { ApiRequestContext, Assistant, JsonRecord } from "../api/types.ts";
import { ApiError } from "../api/types.ts";
import { createRelation } from "./ddl.ts";

type VersionRow = { assistant_id: string; version: number; snapshot: string; created_at: string };
type HeadRow = { version: number };

export interface AssistantVersionsExtension {
  recordCreated(assistant: Assistant, context?: ApiRequestContext): Promise<Assistant>;
  /** Call with the pre-update and post-update assistants, in that order. */
  recordUpdated(previous: Assistant, updated: Assistant, context?: ApiRequestContext): Promise<Assistant>;
  decorate(assistant: Assistant, context?: ApiRequestContext): Promise<Assistant>;
  currentVersion(id: string, context?: ApiRequestContext): Promise<number | null>;
  versions(id: string, query: JsonRecord, context?: ApiRequestContext): Promise<Assistant[]>;
  setLatest(id: string, version: number, context?: ApiRequestContext): Promise<Assistant | null>;
  delete(id: string, context?: ApiRequestContext): Promise<void>;
}

const toAssistant = (record: AssistantRecord): Assistant => ({
  assistant_id: record.id,
  graph_id: record.graphId,
  name: record.name,
  description: record.description,
  config: record.config,
  context: {},
  metadata: record.metadata,
  created_at: record.createdAt,
  updated_at: record.updatedAt,
});

const snapshot = (assistant: Assistant, version: number, createdAt: string): Assistant => ({
  ...assistant,
  version,
  context: assistant.context ?? {},
  created_at: createdAt,
  updated_at: createdAt,
});

const parseSnapshot = (row: VersionRow): Assistant => JSON.parse(row.snapshot) as Assistant;
const number = (value: unknown, fallback: number, max: number): number => {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed >= 0 ? Math.min(parsed, max) : fallback;
};
const object = (value: unknown): JsonRecord =>
  value !== null && typeof value === "object" && !Array.isArray(value) ? value as JsonRecord : {};

/** Version snapshots live beside the base assistant table used by graph execution. */
export async function createAssistantVersionsExtension(store: Store): Promise<AssistantVersionsExtension> {
  await createRelation(store, "valida_assistant_versions", `CREATE TABLE IF NOT EXISTS valida_assistant_versions (
    assistant_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    snapshot TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (assistant_id, version)
  )`);
  await createRelation(store, "valida_assistant_heads", `CREATE TABLE IF NOT EXISTS valida_assistant_heads (
    assistant_id TEXT PRIMARY KEY,
    version INTEGER NOT NULL
  )`);

  async function ensureInitial(assistant: Assistant): Promise<void> {
    const first = snapshot(assistant, 1, assistant.created_at);
    await store.exec(sql`INSERT INTO valida_assistant_versions (assistant_id, version, snapshot, created_at)
      VALUES (${assistant.assistant_id}, ${1}, ${JSON.stringify(first)}, ${assistant.created_at})
      ON CONFLICT (assistant_id, version) DO NOTHING`);
    await store.exec(sql`INSERT INTO valida_assistant_heads (assistant_id, version)
      VALUES (${assistant.assistant_id}, ${1}) ON CONFLICT (assistant_id) DO NOTHING`);
  }
  async function activeVersion(id: string): Promise<number | null> {
    const row = (await store.rows<HeadRow>(sql`SELECT version FROM valida_assistant_heads WHERE assistant_id = ${id}`))[0];
    return row ? Number(row.version) : null;
  }
  async function versionRow(id: string, version: number): Promise<VersionRow | null> {
    return (await store.rows<VersionRow>(sql`SELECT * FROM valida_assistant_versions
      WHERE assistant_id = ${id} AND version = ${version} LIMIT 1`))[0] ?? null;
  }

  return {
    async recordCreated(assistant): Promise<Assistant> {
      await ensureInitial(assistant);
      return { ...assistant, version: 1, context: assistant.context ?? {} };
    },
    async recordUpdated(previous, updated): Promise<Assistant> {
      if (previous.assistant_id !== updated.assistant_id) throw new ApiError(422, "Assistant ID cannot change across versions");
      await ensureInitial(previous);
      for (let attempt = 0; attempt < 20; attempt++) {
        const rows = await store.rows<{ version: number }>(sql`SELECT MAX(version) AS version
          FROM valida_assistant_versions WHERE assistant_id = ${updated.assistant_id}`);
        const next = Number(rows[0]?.version ?? 0) + 1;
        const stamp = new Date().toISOString();
        const updatedWithContext = { ...updated, context: updated.context ?? previous.context ?? {} };
        const value = snapshot(updatedWithContext, next, stamp);
        const inserted = await store.rows<{ version: number }>(sql`INSERT INTO valida_assistant_versions
          (assistant_id, version, snapshot, created_at)
          VALUES (${updated.assistant_id}, ${next}, ${JSON.stringify(value)}, ${stamp})
          ON CONFLICT (assistant_id, version) DO NOTHING RETURNING version`);
        if (!inserted.length) continue;
        await store.exec(sql`INSERT INTO valida_assistant_heads (assistant_id, version)
          VALUES (${updated.assistant_id}, ${next})
          ON CONFLICT (assistant_id) DO UPDATE SET version = excluded.version`);
        return { ...updatedWithContext, version: next };
      }
      throw new ApiError(409, "Assistant version changed concurrently; retry update");
    },
    async decorate(assistant): Promise<Assistant> {
      await ensureInitial(assistant);
      const version = (await activeVersion(assistant.assistant_id)) ?? 1;
      const row = await versionRow(assistant.assistant_id, version);
      const saved = row ? parseSnapshot(row) : assistant;
      return { ...saved, ...assistant, context: saved.context ?? {}, version };
    },
    async currentVersion(id): Promise<number | null> {
      const assistant = await store.getAssistant(id);
      if (!assistant) return null;
      await ensureInitial(toAssistant(assistant));
      return activeVersion(id);
    },
    async versions(id, query): Promise<Assistant[]> {
      const assistant = await store.getAssistant(id);
      if (!assistant) throw new ApiError(404, `Assistant '${id}' not found`);
      await ensureInitial(toAssistant(assistant));
      const rows = await store.rows<VersionRow>(sql`SELECT * FROM valida_assistant_versions
        WHERE assistant_id = ${id} ORDER BY version DESC`);
      const filter = object(query.metadata);
      const matches = rows.map(parseSnapshot).filter((value) =>
        Object.entries(filter).every(([key, expected]) => JSON.stringify(value.metadata[key]) === JSON.stringify(expected)));
      const limit = number(query.limit, 10, 1000);
      const offset = number(query.offset, 0, Number.MAX_SAFE_INTEGER);
      return matches.slice(offset, offset + limit);
    },
    async setLatest(id, version): Promise<Assistant | null> {
      const assistant = await store.getAssistant(id);
      if (!assistant) return null;
      await ensureInitial(toAssistant(assistant));
      const row = await versionRow(id, version);
      if (!row) return null;
      const saved = parseSnapshot(row);
      const stamp = new Date().toISOString();
      await store.exec(sql`UPDATE assistants SET
        graph_id = ${saved.graph_id}, name = ${saved.name}, description = ${saved.description ?? null},
        config = ${JSON.stringify(saved.config)}, metadata = ${JSON.stringify(saved.metadata)},
        updated_at = ${stamp} WHERE id = ${id}`);
      await store.exec(sql`UPDATE valida_assistant_heads SET version = ${version} WHERE assistant_id = ${id}`);
      return { ...saved, created_at: assistant.createdAt, updated_at: stamp, version };
    },
    async delete(id): Promise<void> {
      await store.exec(sql`DELETE FROM valida_assistant_heads WHERE assistant_id = ${id}`);
      await store.exec(sql`DELETE FROM valida_assistant_versions WHERE assistant_id = ${id}`);
    },
  };
}
