import { sql } from "drizzle-orm";
import type { Store, ThreadRecord, ThreadTtlStrategy } from "../db/index.ts";
import { ApiError } from "../api/types.ts";

export interface ThreadPrunerOptions {
  /** Keep stateless run results and replayable events this long after the last thread update. */
  retentionMs?: number;
  /** Maximum number of threads examined in one sweep. */
  sweepLimit?: number;
  /** Maximum number of expired TTL rows examined in one sweep. */
  ttlSweepLimit?: number;
}

type Candidate = Pick<ThreadRecord, "id" | "status" | "metadata" | "updatedAt">;
type TtlCandidate = Candidate & { strategy: ThreadTtlStrategy; ttlMinutes: number; expiresAt: string };
const MAX_BATCH = 100;
const DEFAULT_RETENTION_MS = 24 * 60 * 60 * 1_000;
const DEFAULT_SWEEP_LIMIT = 100;

function candidate(row: Record<string, unknown>): Candidate {
  let metadata: Record<string, unknown> = {};
  try { metadata = JSON.parse(String(row.metadata)); } catch { /* Ignore malformed metadata. */ }
  return {
    id: String(row.id), status: row.status as ThreadRecord["status"],
    metadata, updatedAt: String(row.updated_at),
  };
}

function ttlCandidate(row: Record<string, unknown>): TtlCandidate {
  return { ...candidate(row), strategy: String(row.strategy) as ThreadTtlStrategy,
    ttlMinutes: Number(row.ttl_minutes), expiresAt: String(row.expires_at) };
}

/** Claims finished threads before deleting their runs, events, and checkpoints. */
export class ThreadPruner {
  private readonly retentionMs: number;
  private readonly sweepLimit: number;
  private readonly ttlSweepLimit: number;
  private timer?: ReturnType<typeof setInterval>;
  private sweeping = false;
  private ttlSweeping = false;

  constructor(private readonly store: Store, options: ThreadPrunerOptions = {}) {
    this.retentionMs = options.retentionMs ?? DEFAULT_RETENTION_MS;
    this.sweepLimit = options.sweepLimit ?? DEFAULT_SWEEP_LIMIT;
    this.ttlSweepLimit = options.ttlSweepLimit ?? 1_000;
    if (!Number.isFinite(this.retentionMs) || this.retentionMs < 0) {
      throw new Error("Stateless thread retention must be a non-negative number of milliseconds");
    }
    if (!Number.isSafeInteger(this.sweepLimit) || this.sweepLimit < 1 || this.sweepLimit > 1_000) {
      throw new Error("Thread sweep limit must be an integer from 1 to 1000");
    }
    if (!Number.isSafeInteger(this.ttlSweepLimit) || this.ttlSweepLimit < 1 || this.ttlSweepLimit > 10_000) {
      throw new Error("Thread TTL sweep limit must be an integer from 1 to 10000");
    }
  }

  private async claim(row: Candidate, ttl?: { expiresAt: string; now: string }): Promise<boolean> {
    // An end event is written after graph execution finishes. Checking it closes
    // the small gap between a terminal run status and its final event write.
    // Cancelled jobs are excluded because a graph node may still be unwinding.
    const rows = await this.store.rows<{ id: string }>(sql`UPDATE threads SET status = ${"busy"}
      WHERE id = ${row.id} AND status = ${row.status}
      ${ttl ? sql`AND EXISTS (SELECT 1 FROM thread_ttl t WHERE t.thread_id = threads.id
        AND t.expires_at = ${ttl.expiresAt} AND t.expires_at <= ${ttl.now})` : sql``}
      AND NOT EXISTS (SELECT 1 FROM runs r WHERE r.thread_id = ${row.id}
        AND (r.status NOT IN (${"success"}, ${"error"}, ${"interrupted"})
          OR NOT EXISTS (SELECT 1 FROM events e WHERE e.run_id = r.id AND e.event = ${"end"})))
      RETURNING id`);
    return rows.length === 1;
  }

  private async delete(row: Candidate, ttl?: { expiresAt: string; now: string }): Promise<boolean> {
    if (!["idle", "error", "interrupted"].includes(row.status)) return false;
    if (!await this.claim(row, ttl)) return false;
    try {
      await this.store.deleteThread(row.id);
      return true;
    } catch (error) {
      await this.store.exec(sql`UPDATE threads SET status = ${row.status}
        WHERE id = ${row.id} AND status = ${"busy"}`);
      throw error;
    }
  }

  /** Preserve one checkpoint per namespace and its pending writes, atomically. */
  private async keepLatest(row: Candidate, ttl?: { expiresAt: string; now: string; ttlMinutes: number }): Promise<boolean> {
    if (!["idle", "error", "interrupted"].includes(row.status)) return false;
    if (!await this.claim(row, ttl)) return false;
    try {
      await this.store.transaction([
        sql`DELETE FROM checkpoints WHERE thread_id = ${row.id} AND id NOT IN (
          SELECT id FROM checkpoints WHERE thread_id = ${row.id}
          ORDER BY step DESC, created_at DESC, id DESC LIMIT 1)`,
        sql`UPDATE checkpoints SET parent_id = ${null} WHERE thread_id = ${row.id}`,
        sql`DELETE FROM lg_checkpoints WHERE thread_id = ${row.id}
          AND checkpoint_id <> (SELECT MAX(newest.checkpoint_id) FROM lg_checkpoints AS newest
            WHERE newest.thread_id = lg_checkpoints.thread_id
              AND newest.checkpoint_ns = lg_checkpoints.checkpoint_ns)`,
        sql`DELETE FROM lg_writes WHERE thread_id = ${row.id} AND NOT EXISTS (
          SELECT 1 FROM lg_checkpoints AS kept WHERE kept.thread_id = lg_writes.thread_id
            AND kept.checkpoint_ns = lg_writes.checkpoint_ns
            AND kept.checkpoint_id = lg_writes.checkpoint_id)`,
        sql`UPDATE lg_checkpoints SET parent_id = ${null} WHERE thread_id = ${row.id}`,
        ...(ttl ? [sql`UPDATE thread_ttl SET expires_at =
          ${new Date(new Date(ttl.now).getTime() + ttl.ttlMinutes * 60_000).toISOString()}
          WHERE thread_id = ${row.id} AND expires_at = ${ttl.expiresAt}`] : []),
        sql`UPDATE threads SET status = ${row.status} WHERE id = ${row.id} AND status = ${"busy"}`,
      ]);
      return true;
    } catch (error) {
      await this.store.exec(sql`UPDATE threads SET status = ${row.status}
        WHERE id = ${row.id} AND status = ${"busy"}`);
      throw error;
    }
  }

  /** Explicit SDK-compatible pruning. Unknown, invisible, or active IDs are skipped. */
  async prune(ids: string[], visible: (thread: ThreadRecord) => boolean,
    strategy: "delete" | "keep_latest" = "delete"): Promise<number> {
    if (!Array.isArray(ids) || ids.length > 1_000 || !ids.every(id => typeof id === "string" && id.length > 0)) {
      throw new ApiError(422, "thread_ids must be an array of at most 1000 non-empty strings");
    }
    let count = 0;
    for (const id of new Set(ids)) {
      const thread = await this.store.getThread(id);
      if (!thread || !visible(thread)) continue;
      if (await (strategy === "keep_latest" ? this.keepLatest(thread) : this.delete(thread))) count++;
    }
    return count;
  }

  /** Delete at most one bounded batch of old, finished stateless threads. */
  async sweep(now = Date.now()): Promise<number> {
    if (this.sweeping) return 0;
    this.sweeping = true;
    try {
      const cutoff = new Date(now - this.retentionMs).toISOString();
      const rows = await this.store.rows<Record<string, unknown>>(sql`SELECT id, status, metadata, updated_at
        FROM threads WHERE status IN (${"idle"}, ${"error"})
        AND updated_at < ${cutoff} AND metadata LIKE ${'%"_ephemeral":true%'}
        AND NOT EXISTS (SELECT 1 FROM runs r WHERE r.thread_id = threads.id
          AND (r.status NOT IN (${"success"}, ${"error"}, ${"interrupted"})
            OR NOT EXISTS (SELECT 1 FROM events e WHERE e.run_id = r.id AND e.event = ${"end"})))
        ORDER BY updated_at ASC, id ASC LIMIT ${this.sweepLimit}`);
      let count = 0;
      for (const raw of rows) {
        const row = candidate(raw);
        if (row.metadata._ephemeral !== true) continue;
        if (await this.delete(row)) count++;
      }
      return count;
    } finally {
      this.sweeping = false;
    }
  }

  /** Apply each expired thread's TTL strategy without cancelling active runs. */
  async sweepExpired(now = Date.now()): Promise<{ deleted: number; pruned: number }> {
    if (this.ttlSweeping) return { deleted: 0, pruned: 0 };
    this.ttlSweeping = true;
    try {
      const stamp = new Date(now).toISOString();
      const rows = await this.store.rows<Record<string, unknown>>(sql`SELECT th.id, th.status,
        th.metadata, th.updated_at, t.strategy, t.ttl_minutes, t.expires_at
        FROM thread_ttl t JOIN threads th ON th.id = t.thread_id
        WHERE t.expires_at <= ${stamp} AND th.status IN (${"idle"}, ${"error"}, ${"interrupted"})
        AND NOT EXISTS (SELECT 1 FROM runs r WHERE r.thread_id = th.id
          AND (r.status NOT IN (${"success"}, ${"error"}, ${"interrupted"})
            OR NOT EXISTS (SELECT 1 FROM events e WHERE e.run_id = r.id AND e.event = ${"end"})))
        ORDER BY t.expires_at ASC, th.id ASC LIMIT ${this.ttlSweepLimit}`);
      let deleted = 0, pruned = 0;
      for (const raw of rows) {
        const row = ttlCandidate(raw);
        if (!Number.isFinite(row.ttlMinutes) || row.ttlMinutes <= 0 ||
          !["delete", "keep_latest"].includes(row.strategy)) continue;
        const ttl = { expiresAt: row.expiresAt, now: stamp, ttlMinutes: row.ttlMinutes };
        if (row.strategy === "delete") {
          if (await this.delete(row, ttl)) deleted++;
        } else if (await this.keepLatest(row, ttl)) pruned++;
      }
      return { deleted, pruned };
    } finally {
      this.ttlSweeping = false;
    }
  }

  start(intervalMs = 5 * 60 * 1_000): void {
    if (this.timer) return;
    if (!Number.isFinite(intervalMs) || intervalMs < 1_000) throw new Error("Thread sweep interval must be at least one second");
    this.timer = setInterval(() => {
      void (async () => {
        try { await this.sweep(); }
        catch (error) { console.error("Stateless thread sweep failed", error); }
        try { await this.sweepExpired(); }
        catch (error) { console.error("Thread TTL sweep failed", error); }
      })();
    }, intervalMs);
    this.timer.unref?.();
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = undefined;
  }
}
