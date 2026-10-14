import type { Store } from "../db/index.js";

/** One recorded run event, live or durable. */
export interface RunEvent { seq: number; event: string; data: unknown; at?: string }

type Log = {
  seq: number;
  buffer: RunEvent[];
  bytes: number;
  sizes: number[];
  pending: RunEvent[];
  waiters: Set<() => void>;
  done: boolean;
  flushing?: Promise<void>;
  flushTimer?: ReturnType<typeof setTimeout>;
  expiry?: ReturnType<typeof setTimeout>;
};

export interface RunEventLogOptions {
  /** Live events kept per run for readers that join late or reconnect. */
  maxEvents?: number;
  maxBytes?: number;
  /** How long a finished run's live events stay readable from memory. */
  retainMs?: number;
  /** Durable events are written in batches at most this far apart. */
  flushMs?: number;
}

const roughSize = (event: string, data: unknown): number => {
  if (data === null || typeof data !== "object") return 64;
  // Only state-carrying envelopes are large; measuring every token delta would cost more than it saves.
  if (event === "v2") {
    const method = (data as { method?: unknown }).method;
    if (method !== "values" && method !== "tasks" && method !== "updates") return 512;
  } else if (event === "trace") {
    const kind = (data as { event?: unknown }).event;
    if (kind === "on_chat_model_stream") return 512;
  }
  try { return JSON.stringify(data).length; } catch { return 512; }
};

/**
 * Event log of runs executing in this process. Every event is fanned out to live readers
 * from a bounded memory buffer, as LangGraph's server does with its stream broker; only
 * events marked durable reach the database, written in batches. Runs executing elsewhere,
 * and finished runs once their buffer has expired, are read from the database.
 */
export class RunEventLog {
  private readonly logs = new Map<string, Log>();
  private readonly maxEvents: number;
  private readonly maxBytes: number;
  private readonly retainMs: number;
  private readonly flushMs: number;

  constructor(private readonly store: Store, options: RunEventLogOptions = {}) {
    this.maxEvents = options.maxEvents ?? 10_000;
    this.maxBytes = options.maxBytes ?? 64 * 1024 * 1024;
    this.retainMs = options.retainMs ?? 10 * 60_000;
    this.flushMs = options.flushMs ?? 200;
  }

  /** Start recording a run that executes here, continuing after its stored events. */
  async open(runId: string): Promise<void> {
    const existing = this.logs.get(runId);
    if (existing && !existing.done) return;
    if (existing?.expiry) clearTimeout(existing.expiry);
    const seq = Math.max(await this.store.maxEventSeq(runId), existing?.seq ?? 0);
    this.logs.set(runId, { seq, buffer: [], bytes: 0, sizes: [], pending: [], waiters: new Set(), done: false });
  }

  isLive(runId: string): boolean { return this.logs.has(runId); }

  /** Record an event. Without an open log (e.g. before execution) it is written straight to the database. */
  async append(runId: string, event: string, data: unknown, durable = true): Promise<RunEvent | null> {
    const log = this.logs.get(runId);
    if (!log || log.done) {
      if (!durable) return null;
      const stored = await this.store.appendEvent(runId, event, data);
      if (log) log.seq = Math.max(log.seq, stored.seq);
      return stored;
    }
    const record: RunEvent = { seq: ++log.seq, event, data, at: new Date().toISOString() };
    const size = roughSize(event, data);
    log.buffer.push(record);
    log.sizes.push(size);
    log.bytes += size;
    while (log.buffer.length > 1 && (log.buffer.length > this.maxEvents || log.bytes > this.maxBytes)) {
      log.buffer.shift();
      log.bytes -= log.sizes.shift()!;
    }
    if (durable) {
      log.pending.push(record);
      log.flushTimer ??= setTimeout(() => { void this.flush(runId); }, this.flushMs);
    }
    this.wake(log);
    return record;
  }

  /** Persist an event that was first recorded as live only, e.g. a run's final state. */
  keep(runId: string, record: RunEvent): void {
    const log = this.logs.get(runId);
    if (!log || log.done || log.pending.includes(record)) return;
    log.pending.push(record);
    log.flushTimer ??= setTimeout(() => { void this.flush(runId); }, this.flushMs);
  }

  /** Write queued durable events in one transaction. */
  async flush(runId: string): Promise<void> {
    const log = this.logs.get(runId);
    if (!log) return;
    if (log.flushTimer) { clearTimeout(log.flushTimer); log.flushTimer = undefined; }
    while (log.flushing) await log.flushing;
    if (!log.pending.length) return;
    const batch = log.pending.splice(0);
    log.flushing = this.store.insertEvents(runId, batch).finally(() => { log.flushing = undefined; });
    await log.flushing;
  }

  /** Write every queued durable event, e.g. before the database closes. */
  async flushAll(): Promise<void> {
    await Promise.all([...this.logs.keys()].map(runId => this.flush(runId)));
  }

  /** The run stopped executing here: persist what is queued and let its buffer expire. */
  async close(runId: string): Promise<void> {
    const log = this.logs.get(runId);
    if (!log || log.done) return;
    await this.flush(runId);
    log.done = true;
    this.wake(log);
    log.expiry = setTimeout(() => { if (this.logs.get(runId) === log) this.logs.delete(runId); }, this.retainMs);
    log.expiry.unref?.();
  }

  private wake(log: Log): void {
    const waiters = [...log.waiters];
    log.waiters.clear();
    for (const resolve of waiters) resolve();
  }

  /**
   * Replay events after `after` and follow the run until `finished` reports it complete.
   * Live runs are served from memory; older or remote events come from the database.
   */
  async *read(runId: string, options: {
    after?: number; signal?: AbortSignal; pollMs?: number;
    finished: () => Promise<boolean>;
  }): AsyncGenerator<RunEvent> {
    let after = options.after ?? 0;
    for (;;) {
      if (options.signal?.aborted) return;
      const log = this.logs.get(runId);
      if (log) {
        const first = log.buffer[0]?.seq ?? log.seq + 1;
        // Events older than the buffer (stored before execution here, or already trimmed) are durable ones.
        while (after + 1 < first) {
          const batch = await this.store.listEvents(runId, after, 500);
          const older = batch.filter(item => item.seq < first);
          for (const item of older) { after = item.seq; yield item; }
          if (older.length < batch.length || batch.length < 500) { after = Math.max(after, first - 1); break; }
        }
        let caughtUp = true;
        for (const item of log.buffer) {
          if (item.seq <= after) continue;
          after = item.seq;
          yield item;
          caughtUp = false;
          if (options.signal?.aborted) return;
        }
        if (!caughtUp) continue;
        if (log.done) {
          // The buffer may still trail durable events written by another process.
          for (const item of await this.store.listEvents(runId, after, 500)) { after = item.seq; yield item; }
          return;
        }
        await new Promise<void>(resolve => {
          log.waiters.add(resolve);
          options.signal?.addEventListener("abort", () => resolve(), { once: true });
        });
        continue;
      }
      const batch = await this.store.listEvents(runId, after);
      for (const item of batch) { after = item.seq; yield item; }
      if (batch.length) continue;
      if (await options.finished()) return;
      // Another process may open the log meanwhile; keep the remote case responsive but cheap.
      await new Promise(resolve => setTimeout(resolve, options.pollMs ?? 100));
    }
  }
}
