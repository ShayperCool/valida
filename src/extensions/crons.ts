import cronParser from "cron-parser";
import { sql } from "drizzle-orm";
import type { Store, RunRecord } from "../db/index.ts";
import type { GraphRuntime } from "../engine/index.ts";
import type { ApiRequestContext, JsonRecord, PlatformAdapter } from "../api/types.ts";
import { ApiError } from "../api/types.ts";
import { currentUser } from "../auth.ts";

type CronBackend = NonNullable<PlatformAdapter["crons"]>;
type Row = Record<string, unknown>;

export interface CronExtension extends CronBackend {
  /** Poll due cron records. Conditional SQL claims prevent concurrent API instances firing the same due record. */
  tick(): Promise<number>;
  start(intervalMs?: number): void;
  stop(): void;
}

const now = () => new Date().toISOString();
const object = (value: unknown): JsonRecord =>
  value !== null && typeof value === "object" && !Array.isArray(value) ? value as JsonRecord : {};
const parse = (value: unknown): JsonRecord => {
  try { return object(JSON.parse(String(value))); } catch { return {}; }
};
const owner = (context?: ApiRequestContext): string | null => {
  const principal = context?.principal as { identity?: unknown } | undefined;
  return currentUser.getStore()?.identity ??
    (typeof principal?.identity === "string" ? principal.identity : null);
};
const visible = (row: Row, context?: ApiRequestContext) => !owner(context) || row.owner_id === owner(context);
const positive = (value: unknown, fallback: number, max: number) => {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed >= 0 ? Math.min(parsed, max) : fallback;
};
const nonEmpty = (value: unknown, field: string): string => {
  if (typeof value !== "string" || value.length === 0) throw new ApiError(422, `${field} is required`);
  return value;
};
const nextDate = (schedule: string, timezone: string, from: Date): string => {
  try {
    return cronParser.parse(schedule, { currentDate: from, tz: timezone }).next().toDate().toISOString();
  } catch (cause) {
    throw new ApiError(422, `Invalid cron schedule or timezone: ${cause instanceof Error ? cause.message : String(cause)}`);
  }
};
const cron = (row: Row): JsonRecord => {
  const payload = parse(row.payload);
  return {
    cron_id: String(row.cron_id),
    assistant_id: String(row.assistant_id),
    thread_id: row.thread_id == null ? null : String(row.thread_id),
    schedule: String(row.schedule),
    timezone: String(row.timezone),
    enabled: Number(row.enabled) === 1,
    end_time: row.end_time == null ? null : String(row.end_time),
    next_run_date: row.next_run_at == null ? null : String(row.next_run_at),
    on_run_completed: payload.on_run_completed ?? null,
    payload,
    metadata: parse(row.metadata),
    user_id: row.owner_id == null ? null : String(row.owner_id),
    created_at: String(row.created_at),
    updated_at: String(row.updated_at),
  };
};
const apiRun = (run: RunRecord): JsonRecord => ({
  run_id: run.id,
  thread_id: run.threadId,
  assistant_id: run.assistantId ?? run.graphId,
  status: run.status,
  metadata: run.metadata,
  created_at: run.createdAt,
  updated_at: run.updatedAt,
});

/** Persisted cron records with per-fire leases shared by SQLite or PostgreSQL instances. */
export async function createCronExtension(store: Store, runtime: GraphRuntime): Promise<CronExtension> {
  await store.exec(sql.raw(`CREATE TABLE IF NOT EXISTS valida_crons (
    cron_id TEXT PRIMARY KEY,
    assistant_id TEXT NOT NULL,
    thread_id TEXT,
    schedule TEXT NOT NULL,
    timezone TEXT NOT NULL,
    enabled INTEGER NOT NULL,
    payload TEXT NOT NULL,
    metadata TEXT NOT NULL,
    owner_id TEXT,
    next_run_at TEXT,
    end_time TEXT,
    lease_until TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
  )`));
  await store.exec(sql.raw("CREATE INDEX IF NOT EXISTS valida_crons_due ON valida_crons (enabled, next_run_at, lease_until)"));

  async function find(id: string, context?: ApiRequestContext): Promise<Row | null> {
    const row = (await store.rows<Row>(sql`SELECT * FROM valida_crons WHERE cron_id = ${id} LIMIT 1`))[0] ?? null;
    return row && visible(row, context) ? row : null;
  }
  async function graphId(assistantId: string): Promise<string> {
    const assistant = await store.getAssistant(assistantId);
    if (assistant) return assistant.graphId;
    if (runtime.hasGraph(assistantId)) return assistantId;
    throw new ApiError(404, `Assistant '${assistantId}' not found`);
  }
  async function fire(row: Row): Promise<RunRecord> {
    const assistantId = String(row.assistant_id);
    const graph = await graphId(assistantId);
    const payload = parse(row.payload);
    const threadId = row.thread_id == null
      ? (await runtime.createThread({ metadata: { _ephemeral: true, _cron_id: String(row.cron_id) } })).id
      : String(row.thread_id);
    return runtime.startRun({
      threadId,
      graphId: graph,
      assistantId,
      input: payload.input,
      config: object(payload.config),
      metadata: { ...object(payload.metadata), cron_id: String(row.cron_id) },
    });
  }
  function normalizePayload(value: JsonRecord, existing?: Row): { schedule: string; timezone: string; payload: JsonRecord; metadata: JsonRecord; enabled: boolean; endTime: string | null } {
    const prior = existing ? parse(existing.payload) : {};
    const schedule = nonEmpty(value.schedule ?? existing?.schedule, "schedule");
    const timezone = nonEmpty(value.timezone ?? existing?.timezone ?? "UTC", "timezone");
    const input = value.input === undefined ? prior.input : value.input;
    if (!input || typeof input !== "object" || Array.isArray(input)) throw new ApiError(422, "input must be a JSON object");
    const enabled = value.enabled === undefined ? (existing ? Number(existing.enabled) === 1 : true) : value.enabled !== false;
    const rawEnd = value.end_time === undefined ? existing?.end_time : value.end_time;
    const endDate = rawEnd == null ? null : new Date(String(rawEnd));
    if (endDate && Number.isNaN(endDate.getTime())) throw new ApiError(422, "end_time must be a valid timestamp");
    const endTime = endDate?.toISOString() ?? null;
    if (endTime && endTime <= now()) throw new ApiError(422, "end_time must be in the future");
    const metadata = value.metadata === undefined ? (existing ? parse(existing.metadata) : {}) : object(value.metadata);
    const payload = { ...prior, ...value, input, schedule, timezone, metadata, enabled };
    return { schedule, timezone, payload, metadata, enabled, endTime };
  }

  let timer: ReturnType<typeof setInterval> | undefined;
  let ticking = false;
  const extension: CronExtension = {
    async create(threadId: string | null, value: JsonRecord, context: ApiRequestContext): Promise<JsonRecord> {
      const assistantId = nonEmpty(value.assistant_id, "assistant_id");
      await graphId(assistantId);
      if (threadId) {
        const thread = await store.getThread(threadId);
        if (!thread || (owner(context) && thread.metadata._owner && thread.metadata._owner !== owner(context))) {
          throw new ApiError(404, `Thread '${threadId}' not found`);
        }
      }
      const normalized = normalizePayload(value);
      const id = crypto.randomUUID();
      const stamp = now();
      const next = normalized.enabled ? nextDate(normalized.schedule, normalized.timezone, new Date()) : null;
      await store.exec(sql`INSERT INTO valida_crons
        (cron_id, assistant_id, thread_id, schedule, timezone, enabled, payload, metadata, owner_id,
         next_run_at, end_time, lease_until, created_at, updated_at)
        VALUES (${id}, ${assistantId}, ${threadId}, ${normalized.schedule}, ${normalized.timezone},
        ${normalized.enabled ? 1 : 0}, ${JSON.stringify(normalized.payload)}, ${JSON.stringify(normalized.metadata)},
        ${owner(context)}, ${next}, ${normalized.endTime}, ${null}, ${stamp}, ${stamp})`);
      const row = (await find(id, context))!;
      if (!normalized.enabled) return cron(row);
      try { return apiRun(await fire(row)); }
      catch (cause) {
        await store.exec(sql`DELETE FROM valida_crons WHERE cron_id = ${id}`);
        throw cause;
      }
    },
    async update(id: string, value: JsonRecord, context: ApiRequestContext): Promise<JsonRecord | null> {
      const existing = await find(id, context);
      if (!existing) return null;
      const normalized = normalizePayload(value, existing);
      const next = normalized.enabled
        ? nextDate(normalized.schedule, normalized.timezone, new Date()) : null;
      await store.exec(sql`UPDATE valida_crons SET
        schedule = ${normalized.schedule}, timezone = ${normalized.timezone}, enabled = ${normalized.enabled ? 1 : 0},
        payload = ${JSON.stringify(normalized.payload)}, metadata = ${JSON.stringify(normalized.metadata)},
        next_run_at = ${next}, end_time = ${normalized.endTime}, updated_at = ${now()}
        WHERE cron_id = ${id}`);
      return cron((await find(id, context))!);
    },
    async delete(id: string, context: ApiRequestContext): Promise<boolean> {
      if (!await find(id, context)) return false;
      await store.exec(sql`DELETE FROM valida_crons WHERE cron_id = ${id}`);
      return true;
    },
    async search(value: JsonRecord, context: ApiRequestContext): Promise<JsonRecord[]> {
      const rows = await store.rows<Row>(sql`SELECT * FROM valida_crons ORDER BY created_at DESC, cron_id`);
      const metadata = object(value.metadata);
      const filtered = rows.filter((row) =>
        visible(row, context) &&
        (value.assistant_id == null || row.assistant_id === value.assistant_id) &&
        (value.thread_id == null || row.thread_id === value.thread_id) &&
        (value.enabled == null || (Number(row.enabled) === 1) === value.enabled) &&
        Object.entries(metadata).every(([key, expected]) => JSON.stringify(parse(row.metadata)[key]) === JSON.stringify(expected)));
      const limit = positive(value.limit, 10, 1000);
      const offset = positive(value.offset, 0, Number.MAX_SAFE_INTEGER);
      return filtered.slice(offset, offset + limit).map(cron);
    },
    async count(value: JsonRecord, context: ApiRequestContext): Promise<number> {
      const rows = await store.rows<Row>(sql`SELECT assistant_id, thread_id, enabled, metadata, owner_id FROM valida_crons`);
      const metadata = object(value.metadata);
      return rows.filter((row) =>
        visible(row, context) &&
        (value.assistant_id == null || row.assistant_id === value.assistant_id) &&
        (value.thread_id == null || row.thread_id === value.thread_id) &&
        (value.enabled == null || (Number(row.enabled) === 1) === value.enabled) &&
        Object.entries(metadata).every(([key, expected]) => JSON.stringify(parse(row.metadata)[key]) === JSON.stringify(expected))).length;
    },
    async tick(): Promise<number> {
      if (ticking) return 0;
      ticking = true;
      let fired = 0;
      try {
        const stamp = now();
        const due = await store.rows<Row>(sql`SELECT cron_id FROM valida_crons
          WHERE enabled = ${1} AND next_run_at <= ${stamp}
          AND (lease_until IS NULL OR lease_until < ${stamp})
          ORDER BY next_run_at LIMIT 100`);
        for (const candidate of due) {
          const id = String(candidate.cron_id);
          const leaseUntil = new Date(Date.now() + 120_000).toISOString();
          const claimed = await store.rows<Row>(sql`UPDATE valida_crons SET lease_until = ${leaseUntil}
            WHERE cron_id = ${id} AND enabled = ${1} AND next_run_at <= ${stamp}
            AND (lease_until IS NULL OR lease_until < ${stamp}) RETURNING *`);
          const row = claimed[0];
          if (!row) continue;
          try {
            if (row.end_time != null && String(row.end_time) <= stamp) {
              await store.exec(sql`UPDATE valida_crons SET enabled = ${0}, next_run_at = ${null}, lease_until = ${null}
                WHERE cron_id = ${id}`);
              continue;
            }
            await fire(row);
            fired += 1;
            const next = nextDate(String(row.schedule), String(row.timezone), new Date());
            const enabled = row.end_time == null || next < String(row.end_time);
            await store.exec(sql`UPDATE valida_crons SET next_run_at = ${enabled ? next : null},
              enabled = ${enabled ? 1 : 0}, lease_until = ${null}, updated_at = ${now()}
              WHERE cron_id = ${id} AND lease_until = ${leaseUntil}`);
          } catch (cause) {
            // Keep the due instant and retry after a short lease. The firing may be at least once after a crash.
            await store.exec(sql`UPDATE valida_crons SET lease_until = ${new Date(Date.now() + 30_000).toISOString()}
              WHERE cron_id = ${id} AND lease_until = ${leaseUntil}`);
            console.error("Cron firing failed", id, cause);
          }
        }
        return fired;
      } finally {
        ticking = false;
      }
    },
    start(intervalMs = 1_000): void {
      if (timer) return;
      timer = setInterval(() => { void extension.tick().catch((error) => console.error("Cron scheduler failed", error)); }, intervalMs);
      timer.unref?.();
    },
    stop(): void {
      if (timer) clearInterval(timer);
      timer = undefined;
    },
  };
  return extension;
}
