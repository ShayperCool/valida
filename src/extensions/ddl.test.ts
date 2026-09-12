import { expect, test } from "bun:test";
import type { Store } from "../db/index.ts";
import { createRelation } from "./ddl.ts";

test("fresh PostgreSQL DDL race succeeds only after the relation becomes visible", async () => {
  let checks = 0;
  const duplicate = Object.assign(new Error("duplicate relation"), { code: "23505" });
  const concurrentStore = {
    dialect: "postgres",
    exec: async () => { throw duplicate; },
    rows: async () => { checks += 1; return [{ exists: true }]; },
  } as unknown as Store;
  await expect(createRelation(concurrentStore, "valida_store_items", "CREATE TABLE IF NOT EXISTS valida_store_items (...)")).resolves.toBeUndefined();
  expect(checks).toBe(1);

  const missingStore = {
    dialect: "postgres",
    exec: async () => { throw duplicate; },
    rows: async () => [{ exists: false }],
  } as unknown as Store;
  await expect(createRelation(missingStore, "valida_store_items", "CREATE TABLE IF NOT EXISTS valida_store_items (...)")).rejects.toBe(duplicate);

  const unrelated = Object.assign(new Error("invalid DDL"), { code: "42601" });
  const invalidStore = {
    dialect: "postgres",
    exec: async () => { throw unrelated; },
    rows: async () => { throw new Error("must not verify unrelated errors"); },
  } as unknown as Store;
  await expect(createRelation(invalidStore, "valida_store_items", "broken SQL")).rejects.toBe(unrelated);
});
