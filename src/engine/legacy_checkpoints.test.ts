import { expect, test } from "bun:test";
import { AIMessage, HumanMessage, type BaseMessage } from "@langchain/core/messages";
import { emptyCheckpoint, uuid6 } from "@langchain/langgraph-checkpoint";
import { sql } from "drizzle-orm";
import { createStore } from "../db/index.js";
import { DrizzleCheckpointer } from "./checkpointer.js";
import { migrateLegacyCheckpoints } from "./legacy_checkpoints.js";

test("checkpoints of the previous layout move to the current one unchanged, and the old tables go", async () => {
  const store = await createStore({ dialect: "sqlite", url: ":memory:" });
  const saver = new DrizzleCheckpointer(store);
  // The previous saver: whole checkpoints as base64 text, writes without task paths.
  await store.exec(sql.raw(`CREATE TABLE lg_checkpoints (thread_id TEXT NOT NULL, checkpoint_ns TEXT NOT NULL, checkpoint_id TEXT NOT NULL, parent_id TEXT, checkpoint_type TEXT NOT NULL, checkpoint_blob TEXT NOT NULL, metadata_type TEXT NOT NULL, metadata_blob TEXT NOT NULL, created_at TEXT NOT NULL, PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id))`));
  await store.exec(sql.raw(`CREATE TABLE lg_writes (thread_id TEXT NOT NULL, checkpoint_ns TEXT NOT NULL, checkpoint_id TEXT NOT NULL, task_id TEXT NOT NULL, write_idx INTEGER NOT NULL, channel TEXT NOT NULL, value_type TEXT NOT NULL, value_blob TEXT NOT NULL, PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id, task_id, write_idx))`));
  const b64 = (bytes: Uint8Array) => Buffer.from(bytes).toString("base64");
  const history: BaseMessage[] = [new HumanMessage({ id: "h", content: "hi" })];
  let parent: string | null = null;
  const ids: string[] = [];
  for (let step = 0; step < 3; step++) {
    history.push(new AIMessage({ id: `a${step}`, content: `answer ${step}` }));
    const checkpoint = { ...emptyCheckpoint(), id: uuid6(step), channel_values: { messages: [...history], files: {} },
      channel_versions: { messages: step + 1 } };
    const [type, blob] = await saver.serde.dumpsTyped(checkpoint);
    const [metaType, metaBlob] = await saver.serde.dumpsTyped({ source: "loop", step });
    await store.exec(sql`INSERT INTO lg_checkpoints VALUES ('t', '', ${checkpoint.id}, ${parent}, ${type},
      ${b64(blob)}, ${metaType}, ${b64(metaBlob)}, ${new Date().toISOString()})`);
    const [writeType, writeBlob] = await saver.serde.dumpsTyped([history.at(-1)]);
    await store.exec(sql`INSERT INTO lg_writes VALUES ('t', '', ${checkpoint.id}, 'task', 0, 'messages', ${writeType}, ${b64(writeBlob)})`);
    ids.push(checkpoint.id);
    parent = checkpoint.id;
  }
  await migrateLegacyCheckpoints(store, saver, () => {});
  const latest = await saver.getTuple({ configurable: { thread_id: "t" } });
  expect(latest!.config.configurable!.checkpoint_id).toBe(ids[2]);
  expect(latest!.parentConfig!.configurable!.checkpoint_id).toBe(ids[1]);
  expect((latest!.checkpoint.channel_values.messages as AIMessage[]).map(message => message.content))
    .toEqual(["hi", "answer 0", "answer 1", "answer 2"]);
  expect(latest!.metadata).toMatchObject({ source: "loop", step: 2 });
  expect((latest!.pendingWrites![0]![2] as AIMessage[])[0]!.content).toBe("answer 2");
  // Four distinct messages, though every checkpoint carried the whole history.
  expect((await store.rows<{ n: number }>(sql`SELECT COUNT(*) AS n FROM lg_messages`))[0]!.n).toBe(4);
  expect(await store.rows(sql`SELECT name FROM sqlite_master WHERE name IN ('lg_checkpoints', 'lg_writes')`)).toEqual([]);
  // A second start finds nothing left to move.
  await migrateLegacyCheckpoints(store, saver, () => { throw new Error("nothing to migrate"); });
  await store.close();
});
