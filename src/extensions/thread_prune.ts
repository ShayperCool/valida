import { sql } from "drizzle-orm";
import type { Store, ThreadRecord } from "../db/index.ts";
import { ApiError } from "../api/types.ts";

export interface ThreadPrunerOptions {
  /** Keep stateless run results and replayable events this long after the last thread update. */
  retentionMs?: number;
  /** Maximum number of threads examined in one sweep. */
  sweepLimit?: number;
}

type Candidate = Pick<ThreadRecord, "id" | "status" | "metadata" | "updatedAt">;
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

/** Claims finished threads before deleting their runs, events, and checkpoints. */
export class ThreadPruner {
  private readonly retentionMs: number;
  private readonly sweepLimit: number;
  private timer?: ReturnType<typeof setInterval>;
  private sweeping = false;

  constructor(private readonly store: Store, options: ThreadPrunerOptions = {}) {
    this.retentionMs = options.retentionMs ?? DEFAULT_RETENTION_MS;
    this.sweepLimit = options.sweepLimit ?? DEFAULT_SWEEP_LIMIT;
    if (!Number.isFinite(this.retentionMs) || this.retentionMs < 0) {
      throw new Error("Stateless thread retention must be a non-negative number of milliseconds");
    }
    if (!Number.isSafeInteger(this.sweepLimit) || this.sweepLimit < 1 || this.sweepLimit > 1_000) {
      throw new Error("Thread sweep limit must be an integer from 1 to 1000");
    }
  }

  private async claim(row: Candidate): Promise<boolean> {
    // An end event is written after graph execution finishes. Checking it closes
    // the small gap between a terminal run status and its final event write.
    // Cancelled jobs are excluded because a graph node may still be unwinding.
    const rows = await this.store.rows<{ id: string }>(sql`UPDATE threads SET status = ${"busy"}
      WHERE id = ${row.id} AND status = ${row.status}
      AND NOT EXISTS (SELECT 1 FROM runs r WHERE r.thread_id = ${row.id}
        AND (r.status NOT IN (${"success"}, ${"error"}, ${"interrupted"})
          OR NOT EXISTS (SELECT 1 FROM events e WHERE e.run_id = r.id AND e.event = ${"end"})))
      RETURNING id`);
    return rows.length === 1;
  }

  private async delete(row: Candidate): Promise<boolean> {
    if (!["idle", "error", "interrupted"].includes(row.status)) return false;
    if (!await this.claim(row)) return false;
    try {
      await this.store.deleteThread(row.id);
      return true;
    } catch (error) {
      await this.store.exec(sql`UPDATE threads SET status = ${row.status}
        WHERE id = ${row.id} AND status = ${"busy"}`);
      throw error;
    }
  }

  /** Explicit SDK-compatible bulk deletion. Unknown or invisible IDs are skipped. */
  async prune(ids: string[], visible: (thread: ThreadRecord) => boolean): Promise<number> {
    if (!Array.isArray(ids) || ids.length > 1_000 || !ids.every(id => typeof id === "string" && id.length > 0)) {
      throw new ApiError(422, "thread_ids must be an array of at most 1000 non-empty strings");
    }
    let count = 0;
    for (const id of new Set(ids)) {
      const thread = await this.store.getThread(id);
      if (!thread || !visible(thread)) continue;
      if (await this.delete(thread)) count++;
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

  start(intervalMs = 5 * 60 * 1_000): void {
    if (this.timer) return;
    if (!Number.isFinite(intervalMs) || intervalMs < 1_000) throw new Error("Thread sweep interval must be at least one second");
    this.timer = setInterval(() => {
      void this.sweep().catch(error => console.error("Stateless thread sweep failed", error));
    }, intervalMs);
    this.timer.unref?.();
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = undefined;
  }
}
