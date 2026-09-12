import { BaseCheckpointSaver, WRITES_IDX_MAP, type Checkpoint, type CheckpointListOptions, type CheckpointMetadata, type CheckpointTuple, type PendingWrite } from "@langchain/langgraph-checkpoint";
import type { RunnableConfig } from "@langchain/core/runnables";
import { sql } from "drizzle-orm";
import type { Store } from "../db/index.js";

interface SavedCheckpoint {
  thread_id: string; checkpoint_ns: string; checkpoint_id: string; parent_id: string | null;
  checkpoint_type: string; checkpoint_blob: string; metadata_type: string; metadata_blob: string;
}
interface SavedWrite {
  task_id: string; channel: string; value_type: string; value_blob: string;
}
const b64 = (bytes: Uint8Array) => Buffer.from(bytes).toString("base64");
const unb64 = (value: string) => Buffer.from(value, "base64");

/** Durable LangGraph checkpointer, including pending writes needed for HITL replay. */
export class DrizzleCheckpointer extends BaseCheckpointSaver {
  constructor(private readonly store: Store) { super(); }

  async getTuple(config: RunnableConfig): Promise<CheckpointTuple | undefined> {
    const threadId = config.configurable?.thread_id as string | undefined;
    if (!threadId) return undefined;
    const ns = String(config.configurable?.checkpoint_ns ?? "");
    const checkpointId = config.configurable?.checkpoint_id as string | undefined;
    const rows = checkpointId
      ? await this.store.rows<SavedCheckpoint>(sql`SELECT * FROM lg_checkpoints WHERE thread_id = ${threadId} AND checkpoint_ns = ${ns} AND checkpoint_id = ${checkpointId}`)
      : await this.store.rows<SavedCheckpoint>(sql`SELECT * FROM lg_checkpoints WHERE thread_id = ${threadId} AND checkpoint_ns = ${ns} ORDER BY checkpoint_id DESC LIMIT 1`);
    if (!rows[0]) return undefined;
    return this.inflate(rows[0]);
  }

  private async inflate(row: SavedCheckpoint): Promise<CheckpointTuple> {
    const writes = await this.store.rows<SavedWrite>(sql`SELECT task_id, channel, value_type, value_blob FROM lg_writes
      WHERE thread_id = ${row.thread_id} AND checkpoint_ns = ${row.checkpoint_ns} AND checkpoint_id = ${row.checkpoint_id}
      ORDER BY task_id, write_idx`);
    const pendingWrites = await Promise.all(writes.map(async w => [
      w.task_id, w.channel, await this.serde.loadsTyped(w.value_type, unb64(w.value_blob)),
    ] as [string, string, unknown]));
    return {
      config: { configurable: { thread_id: row.thread_id, checkpoint_ns: row.checkpoint_ns, checkpoint_id: row.checkpoint_id } },
      checkpoint: await this.serde.loadsTyped(row.checkpoint_type, unb64(row.checkpoint_blob)) as Checkpoint,
      metadata: await this.serde.loadsTyped(row.metadata_type, unb64(row.metadata_blob)) as CheckpointMetadata,
      pendingWrites,
      ...(row.parent_id ? { parentConfig: { configurable: { thread_id: row.thread_id, checkpoint_ns: row.checkpoint_ns, checkpoint_id: row.parent_id } } } : {}),
    };
  }

  async *list(config: RunnableConfig, options: CheckpointListOptions = {}): AsyncGenerator<CheckpointTuple> {
    const threadId = config.configurable?.thread_id as string | undefined;
    const ns = config.configurable?.checkpoint_ns as string | undefined;
    const rows = threadId
      ? await this.store.rows<SavedCheckpoint>(sql`SELECT * FROM lg_checkpoints WHERE thread_id = ${threadId} ORDER BY checkpoint_id DESC`)
      : await this.store.rows<SavedCheckpoint>(sql`SELECT * FROM lg_checkpoints ORDER BY checkpoint_id DESC`);
    let count = 0;
    for (const row of rows) {
      if (ns !== undefined && row.checkpoint_ns !== ns) continue;
      if (config.configurable?.checkpoint_id && row.checkpoint_id !== config.configurable.checkpoint_id) continue;
      if (options.before?.configurable?.checkpoint_id && row.checkpoint_id >= options.before.configurable.checkpoint_id) continue;
      const tuple = await this.inflate(row);
      if (options.filter && !Object.entries(options.filter).every(([key, value]) => (tuple.metadata as Record<string, unknown>)?.[key] === value)) continue;
      yield tuple;
      if (++count >= (options.limit ?? Infinity)) return;
    }
  }

  async put(config: RunnableConfig, checkpoint: Checkpoint, metadata: CheckpointMetadata): Promise<RunnableConfig> {
    const threadId = config.configurable?.thread_id as string | undefined;
    if (!threadId) throw new Error("LangGraph checkpoint requires configurable.thread_id");
    const ns = String(config.configurable?.checkpoint_ns ?? "");
    const parentId = (config.configurable?.checkpoint_id as string | undefined) ?? null;
    const [[checkpointType, checkpointBlob], [metadataType, metadataBlob]] = await Promise.all([
      this.serde.dumpsTyped(checkpoint), this.serde.dumpsTyped(metadata),
    ]);
    await this.store.exec(sql`INSERT INTO lg_checkpoints
      (thread_id, checkpoint_ns, checkpoint_id, parent_id, checkpoint_type, checkpoint_blob, metadata_type, metadata_blob, created_at)
      VALUES (${threadId}, ${ns}, ${checkpoint.id}, ${parentId}, ${checkpointType}, ${b64(checkpointBlob)},
        ${metadataType}, ${b64(metadataBlob)}, ${new Date().toISOString()})
      ON CONFLICT (thread_id, checkpoint_ns, checkpoint_id) DO UPDATE SET
      parent_id = EXCLUDED.parent_id, checkpoint_type = EXCLUDED.checkpoint_type,
      checkpoint_blob = EXCLUDED.checkpoint_blob, metadata_type = EXCLUDED.metadata_type,
      metadata_blob = EXCLUDED.metadata_blob`);
    return { configurable: { thread_id: threadId, checkpoint_ns: ns, checkpoint_id: checkpoint.id } };
  }

  async putWrites(config: RunnableConfig, writes: PendingWrite[], taskId: string): Promise<void> {
    const threadId = config.configurable?.thread_id as string | undefined;
    const checkpointId = config.configurable?.checkpoint_id as string | undefined;
    if (!threadId || !checkpointId) throw new Error("LangGraph pending writes require thread_id and checkpoint_id");
    const ns = String(config.configurable?.checkpoint_ns ?? "");
    for (let i = 0; i < writes.length; i++) {
      const [channel, value] = writes[i]!;
      const [type, blob] = await this.serde.dumpsTyped(value);
      const index = WRITES_IDX_MAP[channel] ?? i;
      await this.store.exec(sql`INSERT INTO lg_writes
        (thread_id, checkpoint_ns, checkpoint_id, task_id, write_idx, channel, value_type, value_blob)
        VALUES (${threadId}, ${ns}, ${checkpointId}, ${taskId}, ${index}, ${channel}, ${type}, ${b64(blob)})
        ON CONFLICT (thread_id, checkpoint_ns, checkpoint_id, task_id, write_idx) DO NOTHING`);
    }
  }

  async deleteThread(threadId: string): Promise<void> {
    await this.store.exec(sql`DELETE FROM lg_writes WHERE thread_id = ${threadId}`);
    await this.store.exec(sql`DELETE FROM lg_checkpoints WHERE thread_id = ${threadId}`);
  }
}
