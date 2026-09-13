import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { sql } from "drizzle-orm";
import { createStore, type DatabaseConfig, type Store } from "./index.ts";

async function concurrentWriters(config: DatabaseConfig): Promise<void> {
  const first = await createStore(config);
  const second = await createStore(config);
  const runId = crypto.randomUUID();
  try {
    const entries = await Promise.all(Array.from({ length: 80 }, (_, index) =>
      (index % 2 ? first : second).appendEvent(runId, "test", { index })));
    expect(new Set(entries.map(entry => entry.seq)).size).toBe(80);
    const saved = await first.listEvents(runId, 0, 100);
    expect(saved).toHaveLength(80);
    expect(saved.map(entry => entry.seq)).toEqual(Array.from({ length: 80 }, (_, index) => index + 1));
  } finally {
    await first.exec(sql`DELETE FROM events WHERE run_id = ${runId}`);
    await Promise.all([first.close(), second.close()]);
  }
}

test("concurrent SQLite API writers allocate unique durable SSE IDs", async () => {
  const directory = await mkdtemp(join(tmpdir(), "valida-events-"));
  try { await concurrentWriters({ dialect: "sqlite", url: join(directory, "events.db") }); }
  finally { await rm(directory, { recursive: true, force: true }); }
});

if (process.env.VALIDA_TEST_POSTGRES_URL) {
  test("concurrent PostgreSQL API writers allocate unique durable SSE IDs", async () => {
    await concurrentWriters({ dialect: "postgres", url: process.env.VALIDA_TEST_POSTGRES_URL! });
  });
}
