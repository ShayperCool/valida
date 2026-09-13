import { sql, type SQL } from "drizzle-orm";
import type { Store, ThreadRecord } from "../db/index.ts";
import { ApiError } from "../api/types.ts";

interface LegacyCheckpointRow {
  id: string;
  run_id: string;
  graph_id: string;
  step: number;
  state_values: string;
  next: string;
  tasks: string;
  interrupts: string;
  parent_id: string | null;
  created_at: string;
}

/** Copy a finished thread's complete checkpoint lineage to a new thread. */
export async function copyThread(store: Store, source: ThreadRecord): Promise<ThreadRecord> {
  if (source.status === "busy" || !await store.claimThread(source.id, [source.status])) {
    throw new ApiError(409, `Thread '${source.id}' is busy`);
  }
  try {
    // A source lock blocks new runs. This check also rejects inconsistent rows
    // left by a previous process before copying their possibly incomplete state.
    const active = await store.rows<{ id: string }>(sql`SELECT id FROM runs
      WHERE thread_id = ${source.id} AND status IN (${"pending"}, ${"running"}) LIMIT 1`);
    if (active.length) throw new ApiError(409, `Thread '${source.id}' has an active run`);

    const legacy = await store.rows<LegacyCheckpointRow>(sql`SELECT id, run_id, graph_id, step,
      state_values, next, tasks, interrupts, parent_id, created_at FROM checkpoints
      WHERE thread_id = ${source.id} ORDER BY step, created_at, id`);
    const checkpointIds = new Map(legacy.map(row => [row.id, crypto.randomUUID()]));
    const threadId = crypto.randomUUID();
    const stamp = new Date().toISOString();
    const metadata = { ...source.metadata };
    // A copied thread is explicitly stateful even when its source was a
    // stateless run's temporary backing thread.
    delete metadata._ephemeral;
    const statements: SQL[] = [sql`INSERT INTO threads (id, metadata, status, created_at, updated_at)
      VALUES (${threadId}, ${JSON.stringify(metadata)}, ${source.status}, ${stamp}, ${stamp})`];
    for (const row of legacy) {
      statements.push(sql`INSERT INTO checkpoints (id, thread_id, run_id, graph_id, step,
        state_values, next, tasks, interrupts, parent_id, created_at)
        VALUES (${checkpointIds.get(row.id)!}, ${threadId}, ${row.run_id}, ${row.graph_id},
          ${row.step}, ${row.state_values}, ${row.next}, ${row.tasks}, ${row.interrupts},
          ${row.parent_id ? checkpointIds.get(row.parent_id) ?? null : null}, ${row.created_at})`);
    }
    // Native IDs remain stable inside the new thread. LangGraph's checkpointer
    // keys by (thread_id, namespace, checkpoint_id), and serialized parent
    // references and pending task writes therefore stay consistent.
    statements.push(sql`INSERT INTO lg_checkpoints (thread_id, checkpoint_ns, checkpoint_id,
      parent_id, checkpoint_type, checkpoint_blob, metadata_type, metadata_blob, created_at)
      SELECT ${threadId}, checkpoint_ns, checkpoint_id, parent_id, checkpoint_type,
        checkpoint_blob, metadata_type, metadata_blob, created_at FROM lg_checkpoints
      WHERE thread_id = ${source.id}`);
    statements.push(sql`INSERT INTO lg_writes (thread_id, checkpoint_ns, checkpoint_id,
      task_id, write_idx, channel, value_type, value_blob)
      SELECT ${threadId}, checkpoint_ns, checkpoint_id, task_id, write_idx, channel,
        value_type, value_blob FROM lg_writes WHERE thread_id = ${source.id}`);
    await store.transaction(statements);
    return (await store.getThread(threadId))!;
  } finally {
    await store.exec(sql`UPDATE threads SET status = ${source.status}
      WHERE id = ${source.id} AND status = ${"busy"}`);
  }
}
