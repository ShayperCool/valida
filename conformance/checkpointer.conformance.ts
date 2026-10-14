import { describe } from "vitest";
import { specTest, deltaChannelHistoryTests } from "@langchain/langgraph-checkpoint-validation";
import { sql } from "drizzle-orm";
import { createStore, type Store } from "../src/db/index.js";
import { DrizzleCheckpointer } from "../src/engine/checkpointer.js";

/** The official LangGraph conformance suite for BaseCheckpointSaver, run as written (vitest). */
for (const dialect of process.env.TEST_POSTGRES_URL ? ["sqlite", "postgres"] as const : ["sqlite"] as const) {
  let store: Store;
  const initializer = {
    checkpointerName: `DrizzleCheckpointer (${dialect})`,
    async createCheckpointer() {
      store = await createStore(dialect === "postgres"
        ? { dialect, url: process.env.TEST_POSTGRES_URL! } : { dialect, url: ":memory:" });
      // Each case expects an empty saver; a shared PostgreSQL database keeps earlier rows.
      for (const table of ["lg_checkpoints_v2", "lg_blobs", "lg_checkpoint_writes", "lg_messages"]) {
        await store.exec(sql.raw(`DELETE FROM ${table}`));
      }
      return new DrizzleCheckpointer(store);
    },
    async destroyCheckpointer() { await store.close(); },
  };
  specTest(initializer);
  describe(`${initializer.checkpointerName} delta channels`, () => deltaChannelHistoryTests(initializer));
}
