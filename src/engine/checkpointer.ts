import { createHash } from "node:crypto";
import {
  BaseCheckpointSaver, TASKS, WRITES_IDX_MAP, compareWritesSortKeys, maxChannelVersion, writesSortKey,
  type ChannelVersions, type Checkpoint, type CheckpointListOptions, type CheckpointMetadata,
  type CheckpointTuple, type PendingWrite,
} from "@langchain/langgraph-checkpoint";
import type { RunnableConfig } from "@langchain/core/runnables";
import { sql, type SQL } from "drizzle-orm";
import type { Store } from "../db/index.js";

interface CheckpointRow {
  thread_id: string; checkpoint_ns: string; checkpoint_id: string; parent_checkpoint_id: string | null;
  checkpoint_type: string; checkpoint: Uint8Array; metadata: string;
}
interface BlobRow { channel: string; version: string; type: string; blob: Uint8Array | null }
interface WriteRow { checkpoint_id: string; task_id: string; task_path: string; idx: number; channel: string; type: string; blob: Uint8Array }
interface MessageRow { hash: string; type: string; blob: Uint8Array }

/** Marks where a stored message was taken out of a serialized value. */
const MESSAGE_REF = "__valida_message_ref__";
const MESSAGE_SYMBOL = Symbol.for("langchain.message");
const bytes = (value: unknown): Uint8Array =>
  value instanceof Uint8Array ? value : typeof value === "string" ? Buffer.from(value, "base64") : new Uint8Array(0);
const isPlain = (value: object) => {
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
};
const isMessage = (value: unknown): boolean =>
  typeof value === "object" && value !== null && (value as Record<symbol, unknown>)[MESSAGE_SYMBOL] === true;
// Send packets carry the state they dispatch with; they are recreated with the same class.
const isSend = (value: object) => (value as { lg_name?: unknown }).lg_name === "Send";

/**
 * LangGraph checkpoint saver following the official PostgresSaver layout: a checkpoint row
 * without channel values, one blob per channel version written only for `newVersions`, and
 * pending writes with their task path, returned in `writesSortKey` order. DeltaChannel
 * channels are omitted from blobs and rebuilt from writes by `getDeltaChannelHistory`.
 *
 * As a storage detail, each serialized message is kept once per thread by content hash and
 * referenced from blobs and writes. Agents re-send the whole history in channel values and
 * Send packets at every step; this keeps storage proportional to the distinct messages.
 */
export class DrizzleCheckpointer extends BaseCheckpointSaver {
  constructor(private readonly store: Store) { super(); }

  /** Like PostgresSaver: a random fraction keeps versions minted by two forks of one checkpoint apart. */
  override getNextVersion(current: number | undefined): number {
    return Math.floor(super.getNextVersion(current) as number) + Math.random();
  }

  /** Serialize a value, storing every message inside it once and leaving a reference in its place. */
  private async dump(threadId: string, value: unknown, messages: Map<string, [string, Uint8Array]>): Promise<[string, Uint8Array]> {
    const extract = async (item: unknown): Promise<unknown> => {
      if (typeof item !== "object" || item === null) return item;
      if (isMessage(item)) {
        const [type, blob] = await this.serde.dumpsTyped(item);
        const hash = createHash("sha256").update(type).update("\0").update(blob).digest("hex");
        messages.set(hash, [type, blob]);
        return { [MESSAGE_REF]: hash };
      }
      if (Array.isArray(item)) return Promise.all(item.map(extract));
      if (isPlain(item) || isSend(item)) {
        const copy = Object.create(Object.getPrototypeOf(item)) as Record<string, unknown>;
        for (const [key, entry] of Object.entries(item)) copy[key] = await extract(entry);
        return copy;
      }
      return item;
    };
    const [type, blob] = await this.serde.dumpsTyped(await extract(value));
    return [type, new Uint8Array(blob)];
  }

  /** Deserialize a value and put the referenced messages back, each as its own instance. */
  private async load(type: string, blob: Uint8Array, messages: Map<string, [string, Uint8Array]>): Promise<unknown> {
    const value = await this.serde.loadsTyped(type, blob);
    const restore = async (item: unknown): Promise<unknown> => {
      if (typeof item !== "object" || item === null) return item;
      if (Array.isArray(item)) {
        for (let i = 0; i < item.length; i++) item[i] = await restore(item[i]);
        return item;
      }
      if (isPlain(item) || isSend(item)) {
        const ref = (item as Record<string, unknown>)[MESSAGE_REF];
        if (typeof ref === "string" && Object.keys(item).length === 1) {
          const stored = messages.get(ref);
          if (!stored) throw new Error(`Stored message ${ref} is missing`);
          return this.serde.loadsTyped(stored[0], stored[1]);
        }
        for (const [key, entry] of Object.entries(item)) (item as Record<string, unknown>)[key] = await restore(entry);
      }
      return item;
    };
    return restore(value);
  }

  /** Load the messages referenced by the given blobs, once per hash. */
  private async messagesFor(threadId: string, blobs: { blob: Uint8Array | null }[]): Promise<Map<string, [string, Uint8Array]>> {
    const hashes = new Set<string>();
    const pattern = new RegExp(`"${MESSAGE_REF}":"([0-9a-f]{64})"`, "g");
    for (const { blob } of blobs) {
      if (!blob?.length) continue;
      for (const match of Buffer.from(blob).toString("latin1").matchAll(pattern)) hashes.add(match[1]!);
    }
    const loaded = new Map<string, [string, Uint8Array]>();
    const list = [...hashes];
    for (let i = 0; i < list.length; i += 500) {
      const rows = await this.store.rows<MessageRow>(sql`SELECT hash, type, blob FROM lg_messages
        WHERE thread_id = ${threadId} AND hash IN (${sql.join(list.slice(i, i + 500).map(hash => sql`${hash}`), sql`, `)})`);
      for (const row of rows) loaded.set(row.hash, [row.type, bytes(row.blob)]);
    }
    return loaded;
  }

  private messageInserts(threadId: string, messages: Map<string, [string, Uint8Array]>, stamp: string): SQL[] {
    return [...messages].map(([hash, [type, blob]]) => sql`INSERT INTO lg_messages (thread_id, hash, type, blob, created_at)
      VALUES (${threadId}, ${hash}, ${type}, ${blob}, ${stamp}) ON CONFLICT (thread_id, hash) DO NOTHING`);
  }

  private async tuple(row: CheckpointRow, pendingSends?: [string, Uint8Array][]): Promise<CheckpointTuple> {
    const checkpoint = await this.serde.loadsTyped(row.checkpoint_type, bytes(row.checkpoint)) as Checkpoint;
    checkpoint.channel_versions ??= {};
    checkpoint.versions_seen ??= {};
    const versions = Object.entries(checkpoint.channel_versions);
    const blobs: BlobRow[] = [];
    for (let i = 0; i < versions.length; i += 200) {
      const pairs = versions.slice(i, i + 200).map(([channel, version]) => sql`(${channel}, ${String(version)})`);
      blobs.push(...await this.store.rows<BlobRow>(sql`SELECT channel, version, type, blob FROM lg_blobs
        WHERE thread_id = ${row.thread_id} AND checkpoint_ns = ${row.checkpoint_ns}
        AND (channel, version) IN (VALUES ${sql.join(pairs, sql`, `)})`));
    }
    const writes = await this.store.rows<WriteRow>(sql`SELECT checkpoint_id, task_id, task_path, idx, channel, type, blob
      FROM lg_checkpoint_writes WHERE thread_id = ${row.thread_id} AND checkpoint_ns = ${row.checkpoint_ns}
      AND checkpoint_id = ${row.checkpoint_id}`);
    const messages = await this.messagesFor(row.thread_id, [...blobs, ...writes]);
    const values: Record<string, unknown> = {};
    for (const blob of blobs) {
      if (blob.type !== "empty") values[blob.channel] = await this.load(blob.type, bytes(blob.blob), messages);
    }
    // Checkpoints before format v4 kept pending sends on the parent's TASKS writes.
    if (pendingSends?.length) {
      const sends = [];
      for (const [type, blob] of pendingSends) sends.push(await this.load(type, blob, await this.messagesFor(row.thread_id, [{ blob }])));
      values[TASKS] = sends;
      checkpoint.channel_versions[TASKS] = versions.length
        ? maxChannelVersion(...Object.values(checkpoint.channel_versions)) : this.getNextVersion(undefined);
    }
    checkpoint.channel_values = values;
    const sorted = writes.map(write => ({ key: writesSortKey(write.task_path, write.task_id, Number(write.idx)), write }))
      .sort((a, b) => compareWritesSortKeys(a.key, b.key));
    const pendingWrites: [string, string, unknown][] = [];
    for (const { write } of sorted) {
      pendingWrites.push([write.task_id, write.channel, await this.load(write.type, bytes(write.blob), messages)]);
    }
    const [metaType, metaBlob] = await this.serde.dumpsTyped(JSON.parse(row.metadata));
    const configurable = { thread_id: row.thread_id, checkpoint_ns: row.checkpoint_ns };
    return {
      config: { configurable: { ...configurable, checkpoint_id: row.checkpoint_id } },
      checkpoint,
      metadata: await this.serde.loadsTyped(metaType, metaBlob) as CheckpointMetadata,
      parentConfig: row.parent_checkpoint_id
        ? { configurable: { ...configurable, checkpoint_id: row.parent_checkpoint_id } } : undefined,
      pendingWrites,
    };
  }

  /** Pending sends of pre-v4 checkpoints, read from the parent checkpoint's TASKS writes. */
  private async pendingSends(row: CheckpointRow, checkpoint: { v?: number }): Promise<[string, Uint8Array][] | undefined> {
    if ((checkpoint.v ?? 4) >= 4 || !row.parent_checkpoint_id) return undefined;
    const writes = await this.store.rows<WriteRow>(sql`SELECT checkpoint_id, task_id, task_path, idx, channel, type, blob
      FROM lg_checkpoint_writes WHERE thread_id = ${row.thread_id} AND checkpoint_id = ${row.parent_checkpoint_id}
      AND channel = ${TASKS}`);
    return writes.map(write => ({ key: writesSortKey(write.task_path, write.task_id, Number(write.idx)), write }))
      .sort((a, b) => compareWritesSortKeys(a.key, b.key))
      .map(({ write }) => [write.type, bytes(write.blob)]);
  }

  private async withSends(row: CheckpointRow): Promise<CheckpointTuple> {
    const header = await this.serde.loadsTyped(row.checkpoint_type, bytes(row.checkpoint)) as { v?: number };
    return this.tuple(row, await this.pendingSends(row, header));
  }

  async getTuple(config: RunnableConfig): Promise<CheckpointTuple | undefined> {
    const threadId = config.configurable?.thread_id as string | undefined;
    if (!threadId) return undefined;
    const ns = String(config.configurable?.checkpoint_ns ?? "");
    const checkpointId = config.configurable?.checkpoint_id as string | undefined;
    const [row] = await this.store.rows<CheckpointRow>(checkpointId
      ? sql`SELECT * FROM lg_checkpoints_v2 WHERE thread_id = ${threadId} AND checkpoint_ns = ${ns} AND checkpoint_id = ${checkpointId}`
      : sql`SELECT * FROM lg_checkpoints_v2 WHERE thread_id = ${threadId} AND checkpoint_ns = ${ns} ORDER BY checkpoint_id DESC LIMIT 1`);
    return row ? this.withSends(row) : undefined;
  }

  async *list(config: RunnableConfig, options: CheckpointListOptions = {}): AsyncGenerator<CheckpointTuple> {
    const { filter, before, limit } = options;
    const where: SQL[] = [];
    const configurable = config?.configurable ?? {};
    if (configurable.thread_id) where.push(sql`thread_id = ${configurable.thread_id}`);
    if (configurable.checkpoint_ns !== undefined && configurable.checkpoint_ns !== null) {
      where.push(sql`checkpoint_ns = ${configurable.checkpoint_ns}`);
    }
    if (configurable.checkpoint_id) where.push(sql`checkpoint_id = ${configurable.checkpoint_id}`);
    if (before?.configurable?.checkpoint_id !== undefined) where.push(sql`checkpoint_id < ${before.configurable.checkpoint_id}`);
    const clause = where.length ? sql`WHERE ${sql.join(where, sql` AND `)}` : sql``;
    const entries = filter ? Object.entries(filter) : [];
    const matches = (metadata: string) => {
      if (!entries.length) return true;
      const parsed = JSON.parse(metadata) as Record<string, unknown>;
      return entries.every(([key, value]) => JSON.stringify(parsed[key]) === JSON.stringify(value));
    };
    // Metadata filters run here, so pages are read until `limit` matching rows were found.
    let remaining = limit ?? Infinity;
    let offset = 0;
    while (remaining > 0) {
      const page = await this.store.rows<CheckpointRow>(sql`SELECT * FROM lg_checkpoints_v2 ${clause}
        ORDER BY checkpoint_id DESC LIMIT ${100} OFFSET ${offset}`);
      offset += page.length;
      for (const row of page) {
        if (!matches(row.metadata)) continue;
        yield await this.withSends(row);
        if (--remaining <= 0) return;
      }
      if (page.length < 100) return;
    }
  }

  async put(config: RunnableConfig, checkpoint: Checkpoint, metadata: CheckpointMetadata,
    newVersions: ChannelVersions = {}): Promise<RunnableConfig> {
    if (config.configurable === undefined) throw new Error(`Missing "configurable" field in "config" param`);
    const threadId = config.configurable.thread_id as string | undefined;
    if (!threadId) throw new Error(`Missing "thread_id" field in passed "config.configurable".`);
    const ns = String(config.configurable.checkpoint_ns ?? "");
    const parentId = (config.configurable.checkpoint_id as string | undefined) ?? null;
    const stamp = new Date().toISOString();
    const messages = new Map<string, [string, Uint8Array]>();
    const blobs: SQL[] = [];
    for (const [channel, version] of Object.entries(newVersions)) {
      const [type, blob] = channel in checkpoint.channel_values
        ? await this.dump(threadId, checkpoint.channel_values[channel], messages) : ["empty", null];
      blobs.push(sql`INSERT INTO lg_blobs (thread_id, checkpoint_ns, channel, version, type, blob, created_at)
        VALUES (${threadId}, ${ns}, ${channel}, ${String(version)}, ${type}, ${blob}, ${stamp})
        ON CONFLICT (thread_id, checkpoint_ns, channel, version) DO NOTHING`);
    }
    const { channel_values: _, ...header } = checkpoint;
    const [checkpointType, checkpointBlob] = await this.serde.dumpsTyped(header);
    const [, metadataBlob] = await this.serde.dumpsTyped(metadata);
    const metadataJson = new TextDecoder().decode(metadataBlob).replace(/\0/g, "");
    await this.store.transaction([
      ...this.messageInserts(threadId, messages, stamp),
      ...blobs,
      sql`INSERT INTO lg_checkpoints_v2 (thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id, checkpoint_type, checkpoint, metadata, created_at)
        VALUES (${threadId}, ${ns}, ${checkpoint.id}, ${parentId}, ${checkpointType}, ${new Uint8Array(checkpointBlob)}, ${metadataJson}, ${stamp})
        ON CONFLICT (thread_id, checkpoint_ns, checkpoint_id) DO UPDATE SET
        checkpoint_type = EXCLUDED.checkpoint_type, checkpoint = EXCLUDED.checkpoint, metadata = EXCLUDED.metadata`,
    ]);
    return { configurable: { thread_id: threadId, checkpoint_ns: ns, checkpoint_id: checkpoint.id } };
  }

  async putWrites(config: RunnableConfig, writes: PendingWrite[], taskId: string, taskPath = ""): Promise<void> {
    await this.putWritesAt(config, writes.map(([channel, value], i) => ({ idx: WRITES_IDX_MAP[channel] ?? i, channel, value })),
      taskId, taskPath);
  }

  /** Store writes at explicit indices (putWrites numbers them; a migration keeps the original ones). */
  async putWritesAt(config: RunnableConfig, writes: { idx: number; channel: string; value: unknown }[],
    taskId: string, taskPath = ""): Promise<void> {
    const threadId = config.configurable?.thread_id as string | undefined;
    const checkpointId = config.configurable?.checkpoint_id as string | undefined;
    if (!threadId) throw new Error(`Missing "thread_id" field in passed "config.configurable".`);
    if (!checkpointId) throw new Error(`Missing "checkpoint_id" field in passed "config.configurable".`);
    const ns = String(config.configurable?.checkpoint_ns ?? "");
    const stamp = new Date().toISOString();
    // Special channels (errors, interrupts, resume) replace earlier writes at their fixed index.
    const upsert = writes.every(({ channel }) => channel in WRITES_IDX_MAP);
    const messages = new Map<string, [string, Uint8Array]>();
    const statements: SQL[] = [];
    for (const { idx, channel, value } of writes) {
      const [type, blob] = await this.dump(threadId, value, messages);
      statements.push(sql`INSERT INTO lg_checkpoint_writes
        (thread_id, checkpoint_ns, checkpoint_id, task_id, task_path, idx, channel, type, blob, created_at)
        VALUES (${threadId}, ${ns}, ${checkpointId}, ${taskId}, ${taskPath}, ${idx}, ${channel}, ${type}, ${blob}, ${stamp})
        ON CONFLICT (thread_id, checkpoint_ns, checkpoint_id, task_id, idx) ${upsert
          ? sql`DO UPDATE SET channel = EXCLUDED.channel, type = EXCLUDED.type, blob = EXCLUDED.blob`
          : sql`DO NOTHING`}`);
    }
    await this.store.transaction([...this.messageInserts(threadId, messages, stamp), ...statements]);
  }

  /**
   * Keep only the latest checkpoint of each namespace, plus, for DeltaChannel channels, the
   * ancestors back to their nearest stored value, since their state is replayed from those
   * ancestors' writes. Everything else of the thread is deleted, including unreferenced blobs
   * and messages.
   */
  async pruneToLatest(threadId: string): Promise<void> {
    const namespaces = await this.store.rows<{ checkpoint_ns: string }>(sql`SELECT DISTINCT checkpoint_ns
      FROM lg_checkpoints_v2 WHERE thread_id = ${threadId}`);
    const kept: { ns: string; id: string; versions: ChannelVersions }[] = [];
    const roots: { ns: string; id: string }[] = [];
    for (const { checkpoint_ns: ns } of namespaces) {
      let tuple = await this.getTuple({ configurable: { thread_id: threadId, checkpoint_ns: ns } });
      const pending = new Set(Object.keys(tuple?.metadata?.counters_since_delta_snapshot ?? {}));
      while (tuple) {
        kept.push({ ns, id: tuple.checkpoint.id, versions: tuple.checkpoint.channel_versions });
        for (const channel of [...pending]) if (channel in tuple.checkpoint.channel_values) pending.delete(channel);
        if (!pending.size || !tuple.parentConfig) break;
        tuple = await this.getTuple(tuple.parentConfig);
      }
      roots.push({ ns, id: kept.at(-1)!.id });
    }
    const keptIds = (ns: string) => kept.filter(entry => entry.ns === ns).map(entry => sql`${entry.id}`);
    const statements: SQL[] = [];
    for (const { checkpoint_ns: ns } of namespaces) {
      statements.push(sql`DELETE FROM lg_checkpoints_v2 WHERE thread_id = ${threadId} AND checkpoint_ns = ${ns}
        AND checkpoint_id NOT IN (${sql.join(keptIds(ns), sql`, `)})`);
      statements.push(sql`DELETE FROM lg_checkpoint_writes WHERE thread_id = ${threadId} AND checkpoint_ns = ${ns}
        AND checkpoint_id NOT IN (${sql.join(keptIds(ns), sql`, `)})`);
    }
    for (const root of roots) {
      statements.push(sql`UPDATE lg_checkpoints_v2 SET parent_checkpoint_id = ${null}
        WHERE thread_id = ${threadId} AND checkpoint_ns = ${root.ns} AND checkpoint_id = ${root.id}`);
    }
    await this.store.transaction(statements);
    // Blobs are shared by version across checkpoints; drop those no kept checkpoint refers to.
    const referenced = new Set(kept.flatMap(entry => Object.entries(entry.versions)
      .map(([channel, version]) => `${entry.ns}\0${channel}\0${String(version)}`)));
    const blobs = await this.store.rows<{ checkpoint_ns: string; channel: string; version: string }>(sql`SELECT
      checkpoint_ns, channel, version FROM lg_blobs WHERE thread_id = ${threadId}`);
    const stale = blobs.filter(blob => !referenced.has(`${blob.checkpoint_ns}\0${blob.channel}\0${blob.version}`));
    for (let i = 0; i < stale.length; i += 200) {
      await this.store.exec(sql`DELETE FROM lg_blobs WHERE thread_id = ${threadId} AND (checkpoint_ns, channel, version) IN
        (VALUES ${sql.join(stale.slice(i, i + 200).map(blob => sql`(${blob.checkpoint_ns}, ${blob.channel}, ${blob.version})`), sql`, `)})`);
    }
    await this.collectMessages(threadId);
  }

  /** Delete stored messages that no blob or write of the thread refers to any more. */
  async collectMessages(threadId: string): Promise<void> {
    const values = [
      ...await this.store.rows<{ blob: Uint8Array | null }>(sql`SELECT blob FROM lg_blobs WHERE thread_id = ${threadId}`),
      ...await this.store.rows<{ blob: Uint8Array | null }>(sql`SELECT blob FROM lg_checkpoint_writes WHERE thread_id = ${threadId}`),
    ];
    const used = new Set((await this.messagesFor(threadId, values)).keys());
    const stored = await this.store.rows<{ hash: string }>(sql`SELECT hash FROM lg_messages WHERE thread_id = ${threadId}`);
    const unused = stored.map(row => row.hash).filter(hash => !used.has(hash));
    for (let i = 0; i < unused.length; i += 500) {
      await this.store.exec(sql`DELETE FROM lg_messages WHERE thread_id = ${threadId}
        AND hash IN (${sql.join(unused.slice(i, i + 500).map(hash => sql`${hash}`), sql`, `)})`);
    }
  }

  /** Statements that copy a thread's checkpoints under a new thread ID, keeping checkpoint IDs. */
  copyThreadStatements(source: string, target: string): SQL[] {
    return [
      sql`INSERT INTO lg_checkpoints_v2 (thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id, checkpoint_type, checkpoint, metadata, created_at)
        SELECT ${target}, checkpoint_ns, checkpoint_id, parent_checkpoint_id, checkpoint_type, checkpoint, metadata, created_at
        FROM lg_checkpoints_v2 WHERE thread_id = ${source}`,
      sql`INSERT INTO lg_blobs (thread_id, checkpoint_ns, channel, version, type, blob, created_at)
        SELECT ${target}, checkpoint_ns, channel, version, type, blob, created_at FROM lg_blobs WHERE thread_id = ${source}`,
      sql`INSERT INTO lg_checkpoint_writes (thread_id, checkpoint_ns, checkpoint_id, task_id, task_path, idx, channel, type, blob, created_at)
        SELECT ${target}, checkpoint_ns, checkpoint_id, task_id, task_path, idx, channel, type, blob, created_at
        FROM lg_checkpoint_writes WHERE thread_id = ${source}`,
      sql`INSERT INTO lg_messages (thread_id, hash, type, blob, created_at)
        SELECT ${target}, hash, type, blob, created_at FROM lg_messages WHERE thread_id = ${source}`,
    ];
  }

  async deleteThread(threadId: string): Promise<void> {
    await this.store.transaction([
      sql`DELETE FROM lg_checkpoint_writes WHERE thread_id = ${threadId}`,
      sql`DELETE FROM lg_blobs WHERE thread_id = ${threadId}`,
      sql`DELETE FROM lg_checkpoints_v2 WHERE thread_id = ${threadId}`,
      sql`DELETE FROM lg_messages WHERE thread_id = ${threadId}`,
    ]);
  }
}
