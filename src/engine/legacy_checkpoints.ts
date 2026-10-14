import type { Checkpoint, CheckpointMetadata } from "@langchain/langgraph-checkpoint";
import { sql } from "drizzle-orm";
import type { Store } from "../db/index.js";
import type { DrizzleCheckpointer } from "./checkpointer.js";

interface LegacyCheckpoint {
  thread_id: string; checkpoint_ns: string; checkpoint_id: string; parent_id: string | null;
  checkpoint_type: string; checkpoint_blob: string; metadata_type: string; metadata_blob: string;
}
interface LegacyWrite {
  checkpoint_ns: string; checkpoint_id: string; task_id: string; write_idx: number;
  channel: string; value_type: string; value_blob: string;
}
const unb64 = (value: string) => Buffer.from(value, "base64");

async function tableExists(store: Store, table: string): Promise<boolean> {
  const rows = store.dialect === "sqlite"
    ? await store.rows(sql`SELECT name FROM sqlite_master WHERE type = 'table' AND name = ${table}`)
    : await store.rows<{ name: string | null }>(sql`SELECT to_regclass(${table})::text AS name`);
  return rows.some(row => (row as { name?: unknown }).name);
}

/**
 * Move checkpoints written by the previous saver (whole checkpoints as base64 text in
 * lg_checkpoints, writes without task paths in lg_writes) into the current layout, one thread at
 * a time, through the saver itself so values and messages are deduplicated. A thread's old rows
 * are deleted once it has moved, so an interrupted migration resumes where it stopped; the old
 * tables are dropped when empty.
 */
export async function migrateLegacyCheckpoints(store: Store, saver: DrizzleCheckpointer,
  log: (message: string) => void = message => console.log(message)): Promise<void> {
  if (!await tableExists(store, "lg_checkpoints")) return;
  const writesTable = await tableExists(store, "lg_writes");
  const threads = await store.rows<{ thread_id: string }>(writesTable
    ? sql`SELECT thread_id FROM lg_checkpoints UNION SELECT thread_id FROM lg_writes`
    : sql`SELECT DISTINCT thread_id FROM lg_checkpoints`);
  if (threads.length) log(`Moving checkpoints of ${threads.length} threads to the current storage layout`);
  let moved = 0;
  for (const { thread_id: threadId } of threads) {
    const checkpoints = await store.rows<{ checkpoint_ns: string; checkpoint_id: string }>(sql`SELECT checkpoint_ns,
      checkpoint_id FROM lg_checkpoints WHERE thread_id = ${threadId} ORDER BY checkpoint_ns, checkpoint_id`);
    for (const key of checkpoints) {
      // One at a time: a single old checkpoint can hold megabytes of history.
      const [row] = await store.rows<LegacyCheckpoint>(sql`SELECT * FROM lg_checkpoints WHERE thread_id = ${threadId}
        AND checkpoint_ns = ${key.checkpoint_ns} AND checkpoint_id = ${key.checkpoint_id}`);
      const checkpoint = await saver.serde.loadsTyped(row!.checkpoint_type, unb64(row!.checkpoint_blob)) as Checkpoint;
      const metadata = await saver.serde.loadsTyped(row!.metadata_type, unb64(row!.metadata_blob)) as CheckpointMetadata;
      await saver.put({ configurable: { thread_id: threadId, checkpoint_ns: row!.checkpoint_ns,
        ...(row!.parent_id ? { checkpoint_id: row!.parent_id } : {}) } }, checkpoint, metadata, checkpoint.channel_versions ?? {});
    }
    if (writesTable) {
      const writes = await store.rows<LegacyWrite>(sql`SELECT checkpoint_ns, checkpoint_id, task_id, write_idx, channel,
        value_type, value_blob FROM lg_writes WHERE thread_id = ${threadId} ORDER BY checkpoint_ns, checkpoint_id, task_id, write_idx`);
      const groups = new Map<string, LegacyWrite[]>();
      for (const write of writes) {
        const group = `${write.checkpoint_ns}\0${write.checkpoint_id}\0${write.task_id}`;
        groups.set(group, [...groups.get(group) ?? [], write]);
      }
      for (const group of groups.values()) {
        const first = group[0]!;
        const entries = [];
        for (const write of group) entries.push({ idx: Number(write.write_idx), channel: write.channel,
          value: await saver.serde.loadsTyped(write.value_type, unb64(write.value_blob)) });
        // Rows of the old layout have no task path; they sort first, by task id, as the spec defines.
        await saver.putWritesAt({ configurable: { thread_id: threadId, checkpoint_ns: first.checkpoint_ns,
          checkpoint_id: first.checkpoint_id } }, entries, first.task_id, "");
      }
    }
    await store.transaction([
      ...(writesTable ? [sql`DELETE FROM lg_writes WHERE thread_id = ${threadId}`] : []),
      sql`DELETE FROM lg_checkpoints WHERE thread_id = ${threadId}`,
    ]);
    if (++moved % 100 === 0) log(`Moved checkpoints of ${moved}/${threads.length} threads`);
  }
  await store.exec(sql`DROP TABLE IF EXISTS lg_writes`);
  await store.exec(sql`DROP TABLE IF EXISTS lg_checkpoints`);
  if (threads.length) log(`Moved checkpoints of ${threads.length} threads`);
}
