import { Database } from "bun:sqlite";
import { drizzle as sqliteDrizzle } from "drizzle-orm/bun-sqlite";
import { drizzle as pgDrizzle } from "drizzle-orm/postgres-js";
import { sql, type SQL } from "drizzle-orm";
import postgres from "postgres";

export type DatabaseConfig =
  | { dialect: "sqlite"; url?: string }
  | { dialect: "postgres"; url: string };

export type JsonObject = Record<string, unknown>;
export type RunStatus = "pending" | "running" | "interrupted" | "success" | "error" | "cancelled";
export type ThreadStatus = "idle" | "busy" | "interrupted" | "error";

export interface AssistantRecord {
  id: string; graphId: string; name: string; description: string | null;
  config: JsonObject; metadata: JsonObject; createdAt: string; updatedAt: string;
}
export interface ThreadRecord {
  id: string; metadata: JsonObject; status: ThreadStatus; createdAt: string; updatedAt: string;
}
export interface RunRecord {
  id: string; threadId: string; assistantId: string | null; graphId: string;
  status: RunStatus; input: unknown; output: unknown; error: string | null;
  config: JsonObject; metadata: JsonObject; resume: unknown;
  leaseUntil: string | null; createdAt: string; updatedAt: string;
}
export interface CheckpointRecord {
  id: string; threadId: string; runId: string; graphId: string;
  step: number; values: JsonObject; next: string[];
  tasks: unknown[]; interrupts: unknown[]; parentId: string | null; createdAt: string;
}
export interface EventRecord {
  runId: string; seq: number; event: string; data: unknown; createdAt: string;
}

type Raw = Record<string, unknown>;
const now = () => new Date().toISOString();
const encode = (value: unknown) => JSON.stringify(value ?? null);
const decode = <T>(value: unknown, fallback: T): T => {
  if (value == null) return fallback;
  try { return JSON.parse(String(value)) as T; } catch { return fallback; }
};
const one = <T>(rows: T[]) => rows[0] ?? null;
const assistant = (r: Raw): AssistantRecord => ({
  id: String(r.id), graphId: String(r.graph_id), name: String(r.name),
  description: r.description == null ? null : String(r.description),
  config: decode(r.config, {}), metadata: decode(r.metadata, {}),
  createdAt: String(r.created_at), updatedAt: String(r.updated_at),
});
const thread = (r: Raw): ThreadRecord => ({
  id: String(r.id), metadata: decode(r.metadata, {}), status: r.status as ThreadStatus,
  createdAt: String(r.created_at), updatedAt: String(r.updated_at),
});
const run = (r: Raw): RunRecord => ({
  id: String(r.id), threadId: String(r.thread_id), assistantId: r.assistant_id == null ? null : String(r.assistant_id),
  graphId: String(r.graph_id), status: r.status as RunStatus,
  input: decode(r.input, null), output: decode(r.output, null), error: r.error == null ? null : String(r.error),
  config: decode(r.config, {}), metadata: decode(r.metadata, {}), resume: decode(r.resume, null),
  leaseUntil: r.lease_until == null ? null : String(r.lease_until),
  createdAt: String(r.created_at), updatedAt: String(r.updated_at),
});
const checkpoint = (r: Raw): CheckpointRecord => ({
  id: String(r.id), threadId: String(r.thread_id), runId: String(r.run_id),
  graphId: String(r.graph_id), step: Number(r.step), values: decode(r.state_values, {}),
  next: decode(r.next, []), tasks: decode(r.tasks, []), interrupts: decode(r.interrupts, []),
  parentId: r.parent_id == null ? null : String(r.parent_id), createdAt: String(r.created_at),
});
const event = (r: Raw): EventRecord => ({
  runId: String(r.run_id), seq: Number(r.seq), event: String(r.event),
  data: decode(r.data, null), createdAt: String(r.created_at),
});

/** A small Drizzle-backed repository shared by the standalone and distributed runtimes. */
export class Store {
  readonly dialect: "sqlite" | "postgres";
  private sqlite?: Database;
  private sqliteDb?: ReturnType<typeof sqliteDrizzle>;
  private pg?: ReturnType<typeof postgres>;
  private pgDb?: ReturnType<typeof pgDrizzle>;

  constructor(config: DatabaseConfig) {
    this.dialect = config.dialect;
    if (config.dialect === "sqlite") {
      this.sqlite = new Database(config.url ?? ":memory:", { create: true });
      this.sqlite.exec("PRAGMA foreign_keys = ON");
      this.sqlite.exec("PRAGMA busy_timeout = 5000");
      if (config.url && config.url !== ":memory:") this.sqlite.exec("PRAGMA journal_mode = WAL");
      this.sqliteDb = sqliteDrizzle(this.sqlite);
    } else {
      this.pg = postgres(config.url, { max: 10 });
      this.pgDb = pgDrizzle(this.pg);
    }
  }

  async rows<T = Raw>(statement: SQL): Promise<T[]> {
    if (this.sqliteDb) return this.sqliteDb.all(statement) as T[];
    return await this.pgDb!.execute(statement) as unknown as T[];
  }
  async exec(statement: SQL): Promise<void> {
    if (this.sqliteDb) { this.sqliteDb.run(statement); return; }
    await this.pgDb!.execute(statement);
  }

  async migrate(): Promise<void> {
    // Text-encoded JSON keeps the two schemas equivalent and migration between them mechanical.
    const statements = [
      `CREATE TABLE IF NOT EXISTS assistants (id TEXT PRIMARY KEY, graph_id TEXT NOT NULL, name TEXT NOT NULL, description TEXT, config TEXT NOT NULL, metadata TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)`,
      `CREATE TABLE IF NOT EXISTS threads (id TEXT PRIMARY KEY, metadata TEXT NOT NULL, status TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)`,
      `CREATE TABLE IF NOT EXISTS runs (id TEXT PRIMARY KEY, thread_id TEXT NOT NULL, assistant_id TEXT, graph_id TEXT NOT NULL, status TEXT NOT NULL, input TEXT, output TEXT, error TEXT, config TEXT NOT NULL, metadata TEXT NOT NULL, resume TEXT, lease_until TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)`,
      `CREATE TABLE IF NOT EXISTS checkpoints (id TEXT PRIMARY KEY, thread_id TEXT NOT NULL, run_id TEXT NOT NULL, graph_id TEXT NOT NULL, step INTEGER NOT NULL, state_values TEXT NOT NULL, next TEXT NOT NULL, tasks TEXT NOT NULL, interrupts TEXT NOT NULL, parent_id TEXT, created_at TEXT NOT NULL)`,
      `CREATE TABLE IF NOT EXISTS events (run_id TEXT NOT NULL, seq INTEGER NOT NULL, event TEXT NOT NULL, data TEXT NOT NULL, created_at TEXT NOT NULL, PRIMARY KEY (run_id, seq))`,
      `CREATE TABLE IF NOT EXISTS lg_checkpoints (thread_id TEXT NOT NULL, checkpoint_ns TEXT NOT NULL, checkpoint_id TEXT NOT NULL, parent_id TEXT, checkpoint_type TEXT NOT NULL, checkpoint_blob TEXT NOT NULL, metadata_type TEXT NOT NULL, metadata_blob TEXT NOT NULL, created_at TEXT NOT NULL, PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id))`,
      `CREATE TABLE IF NOT EXISTS lg_writes (thread_id TEXT NOT NULL, checkpoint_ns TEXT NOT NULL, checkpoint_id TEXT NOT NULL, task_id TEXT NOT NULL, write_idx INTEGER NOT NULL, channel TEXT NOT NULL, value_type TEXT NOT NULL, value_blob TEXT NOT NULL, PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id, task_id, write_idx))`,
      `CREATE INDEX IF NOT EXISTS runs_thread_created ON runs (thread_id, created_at DESC)`,
      `CREATE INDEX IF NOT EXISTS runs_status_lease ON runs (status, lease_until)`,
      `CREATE INDEX IF NOT EXISTS checkpoints_thread_created ON checkpoints (thread_id, created_at DESC)`,
      `CREATE INDEX IF NOT EXISTS lg_checkpoints_latest ON lg_checkpoints (thread_id, checkpoint_ns, checkpoint_id DESC)`,
    ];
    for (const statement of statements) await this.exec(sql.raw(statement));
  }

  async close(): Promise<void> { this.sqlite?.close(); await this.pg?.end(); }

  async createAssistant(value: Partial<AssistantRecord> & { graphId: string; name?: string }): Promise<AssistantRecord> {
    const id = value.id ?? crypto.randomUUID(), stamp = now();
    await this.exec(sql`INSERT INTO assistants (id, graph_id, name, description, config, metadata, created_at, updated_at)
      VALUES (${id}, ${value.graphId}, ${value.name ?? value.graphId}, ${value.description ?? null},
      ${encode(value.config ?? {})}, ${encode(value.metadata ?? {})}, ${stamp}, ${stamp})`);
    return (await this.getAssistant(id))!;
  }
  async getAssistant(id: string): Promise<AssistantRecord | null> {
    return one((await this.rows(sql`SELECT * FROM assistants WHERE id = ${id}`)).map(assistant));
  }
  async listAssistants(limit = 100, offset = 0): Promise<AssistantRecord[]> {
    return (await this.rows(sql`SELECT * FROM assistants ORDER BY created_at DESC LIMIT ${limit} OFFSET ${offset}`)).map(assistant);
  }
  async updateAssistant(id: string, patch: Partial<Pick<AssistantRecord,"name"|"description"|"config"|"metadata">>): Promise<AssistantRecord | null> {
    const old = await this.getAssistant(id); if (!old) return null;
    await this.exec(sql`UPDATE assistants SET name = ${patch.name ?? old.name}, description = ${patch.description === undefined ? old.description : patch.description},
      config = ${encode(patch.config ?? old.config)}, metadata = ${encode(patch.metadata ?? old.metadata)}, updated_at = ${now()} WHERE id = ${id}`);
    return this.getAssistant(id);
  }
  async deleteAssistant(id: string): Promise<void> { await this.exec(sql`DELETE FROM assistants WHERE id = ${id}`); }

  async createThread(value: { id?: string; metadata?: JsonObject } = {}): Promise<ThreadRecord> {
    const id = value.id ?? crypto.randomUUID(), stamp = now();
    await this.exec(sql`INSERT INTO threads (id, metadata, status, created_at, updated_at)
      VALUES (${id}, ${encode(value.metadata ?? {})}, ${"idle"}, ${stamp}, ${stamp})`);
    return (await this.getThread(id))!;
  }
  async getThread(id: string): Promise<ThreadRecord | null> {
    return one((await this.rows(sql`SELECT * FROM threads WHERE id = ${id}`)).map(thread));
  }
  async listThreads(limit = 100, offset = 0): Promise<ThreadRecord[]> {
    return (await this.rows(sql`SELECT * FROM threads ORDER BY updated_at DESC LIMIT ${limit} OFFSET ${offset}`)).map(thread);
  }
  async updateThread(id: string, patch: { metadata?: JsonObject; status?: ThreadStatus }): Promise<ThreadRecord | null> {
    const old = await this.getThread(id); if (!old) return null;
    await this.exec(sql`UPDATE threads SET metadata = ${encode(patch.metadata ?? old.metadata)}, status = ${patch.status ?? old.status}, updated_at = ${now()} WHERE id = ${id}`);
    return this.getThread(id);
  }
  async claimThread(id: string, expected: ThreadStatus[]): Promise<boolean> {
    if (!expected.length) return false;
    const rows = await this.rows<{ id: string }>(sql`UPDATE threads SET status = ${"busy"}, updated_at = ${now()}
      WHERE id = ${id} AND status IN (${sql.join(expected.map(value => sql`${value}`), sql`, `)}) RETURNING id`);
    return rows.length === 1;
  }
  async deleteThread(id: string): Promise<void> {
    await this.exec(sql`DELETE FROM lg_writes WHERE thread_id = ${id}`);
    await this.exec(sql`DELETE FROM lg_checkpoints WHERE thread_id = ${id}`);
    await this.exec(sql`DELETE FROM events WHERE run_id IN (SELECT id FROM runs WHERE thread_id = ${id})`);
    await this.exec(sql`DELETE FROM checkpoints WHERE thread_id = ${id}`);
    await this.exec(sql`DELETE FROM runs WHERE thread_id = ${id}`);
    await this.exec(sql`DELETE FROM threads WHERE id = ${id}`);
  }

  async createRun(value: {
    id?: string; threadId: string; graphId: string; assistantId?: string | null;
    input?: unknown; config?: JsonObject; metadata?: JsonObject; resume?: unknown;
  }): Promise<RunRecord> {
    const id = value.id ?? crypto.randomUUID(), stamp = now();
    await this.exec(sql`INSERT INTO runs (id, thread_id, assistant_id, graph_id, status, input, output, error, config, metadata, resume, lease_until, created_at, updated_at)
      VALUES (${id}, ${value.threadId}, ${value.assistantId ?? null}, ${value.graphId}, ${"pending"},
      ${encode(value.input)}, ${null}, ${null}, ${encode(value.config ?? {})}, ${encode(value.metadata ?? {})},
      ${encode(value.resume)}, ${null}, ${stamp}, ${stamp})`);
    return (await this.getRun(id))!;
  }
  async getRun(id: string): Promise<RunRecord | null> {
    return one((await this.rows(sql`SELECT * FROM runs WHERE id = ${id}`)).map(run));
  }
  async listRuns(threadId: string, limit = 100): Promise<RunRecord[]> {
    return (await this.rows(sql`SELECT * FROM runs WHERE thread_id = ${threadId} ORDER BY created_at DESC LIMIT ${limit}`)).map(run);
  }
  async claimRun(id: string, leaseMs = 60_000): Promise<boolean> {
    const until = new Date(Date.now() + leaseMs).toISOString(), stamp = now();
    await this.exec(sql`UPDATE runs SET status = ${"running"}, lease_until = ${until}, updated_at = ${stamp}
      WHERE id = ${id} AND (status = ${"pending"} OR (status = ${"running"} AND lease_until < ${stamp}))`);
    const current = await this.getRun(id);
    return current?.status === "running" && current.leaseUntil === until;
  }
  async renewRun(id: string, leaseMs = 60_000): Promise<void> {
    await this.exec(sql`UPDATE runs SET lease_until = ${new Date(Date.now() + leaseMs).toISOString()}, updated_at = ${now()} WHERE id = ${id} AND status = ${"running"}`);
  }
  async updateRun(id: string, patch: Partial<Pick<RunRecord,"status"|"output"|"error"|"resume"|"leaseUntil"|"metadata">>): Promise<RunRecord | null> {
    const old = await this.getRun(id); if (!old) return null;
    await this.exec(sql`UPDATE runs SET status = ${patch.status ?? old.status}, output = ${encode(patch.output === undefined ? old.output : patch.output)},
      error = ${patch.error === undefined ? old.error : patch.error}, resume = ${encode(patch.resume === undefined ? old.resume : patch.resume)},
      lease_until = ${patch.leaseUntil === undefined ? old.leaseUntil : patch.leaseUntil}, metadata = ${encode(patch.metadata ?? old.metadata)},
      updated_at = ${now()} WHERE id = ${id}`);
    return this.getRun(id);
  }
  async cancelRun(id: string): Promise<RunRecord | null> { return this.updateRun(id, { status: "cancelled", leaseUntil: null }); }

  async createCheckpoint(value: Omit<CheckpointRecord,"id"|"createdAt"> & { id?: string }): Promise<CheckpointRecord> {
    const id = value.id ?? crypto.randomUUID(), stamp = now();
    await this.exec(sql`INSERT INTO checkpoints (id, thread_id, run_id, graph_id, step, state_values, next, tasks, interrupts, parent_id, created_at)
      VALUES (${id}, ${value.threadId}, ${value.runId}, ${value.graphId}, ${value.step}, ${encode(value.values)},
      ${encode(value.next)}, ${encode(value.tasks)}, ${encode(value.interrupts)}, ${value.parentId}, ${stamp})`);
    return (await this.getCheckpoint(id))!;
  }
  async getCheckpoint(id: string): Promise<CheckpointRecord | null> {
    return one((await this.rows(sql`SELECT * FROM checkpoints WHERE id = ${id}`)).map(checkpoint));
  }
  async getState(threadId: string): Promise<CheckpointRecord | null> {
    return one((await this.rows(sql`SELECT * FROM checkpoints WHERE thread_id = ${threadId} ORDER BY step DESC, created_at DESC LIMIT 1`)).map(checkpoint));
  }
  async getHistory(threadId: string, limit = 100): Promise<CheckpointRecord[]> {
    return (await this.rows(sql`SELECT * FROM checkpoints WHERE thread_id = ${threadId} ORDER BY step DESC, created_at DESC LIMIT ${limit}`)).map(checkpoint);
  }

  async appendEvent(runId: string, name: string, data: unknown): Promise<EventRecord> {
    const rows = await this.rows<{ seq: number }>(sql`SELECT COALESCE(MAX(seq), 0) + 1 AS seq FROM events WHERE run_id = ${runId}`);
    const seq = Number(rows[0]?.seq ?? 1), stamp = now();
    await this.exec(sql`INSERT INTO events (run_id, seq, event, data, created_at) VALUES (${runId}, ${seq}, ${name}, ${encode(data)}, ${stamp})`);
    return { runId, seq, event: name, data, createdAt: stamp };
  }
  async listEvents(runId: string, after = 0, limit = 100): Promise<EventRecord[]> {
    return (await this.rows(sql`SELECT * FROM events WHERE run_id = ${runId} AND seq > ${after} ORDER BY seq ASC LIMIT ${limit}`)).map(event);
  }
}

export async function createStore(config: DatabaseConfig): Promise<Store> {
  const store = new Store(config);
  await store.migrate();
  return store;
}
