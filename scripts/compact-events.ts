/**
 * Apply the current event storage policy to finished runs recorded before it.
 *
 * Earlier versions stored every event of a compiled graph run: token deltas, the full state of
 * every step (values), task inputs, LangChain callback traces and a legacy duplicate of most
 * envelopes. Runs now keep only what replay needs unless a client asked for those stream modes
 * (see durableV2 in src/engine/index.ts). For finished runs of compiled graphs that recorded no
 * stream modes, this keeps lifecycle, updates, tools, custom events, checkpoint markers and finished
 * messages, and clears the copies of the final state on the run row and its run-level checkpoint.
 *
 *   bun scripts/compact-events.ts <sqlite path or postgres url> [--dry-run]
 *
 * Stop Valida first; afterwards VACUUM (SQLite) reclaims the space.
 */
import { sql } from "drizzle-orm";
import { createStore } from "../src/db/index.ts";

const target = process.argv[2];
const dryRun = process.argv.includes("--dry-run");
if (!target) throw new Error("Usage: bun scripts/compact-events.ts <sqlite path or postgres url> [--dry-run]");
const store = await createStore(target.startsWith("postgres")
  ? { dialect: "postgres", url: target } : { dialect: "sqlite", url: target });
const pg = store.dialect === "postgres";
const field = (path: string[]) => pg
  ? sql.raw(`(data::jsonb #>> '{${path.join(",")}}')`)
  : sql.raw(`json_extract(data, '$.${path.join(".")}')`);

// Finished runs of compiled graphs (they recorded v3 envelopes) without requested stream modes.
const runs = (await store.rows<{ id: string; metadata: string }>(sql`SELECT id, metadata FROM runs
  WHERE status IN ('success', 'error', 'interrupted', 'cancelled')
  AND EXISTS (SELECT 1 FROM events WHERE events.run_id = runs.id AND events.event = 'v2')`))
  .filter(run => !Array.isArray((JSON.parse(run.metadata) as { __stream_modes?: unknown }).__stream_modes))
  .map(run => run.id);
console.log(`${runs.length} finished runs to compact${dryRun ? " (dry run)" : ""}`);

const dropped = (runId: string) => sql`run_id = ${runId} AND (
  event IN ('trace', 'values', 'updates', 'custom', 'tools', 'checkpoints', 'tasks')
  OR (event = 'v2' AND (
    ${field(["method"])} = 'tasks'
    OR (${field(["method"])} = 'messages' AND ${field(["params", "data", "event"])} = 'content-block-delta')
    OR ${field(["method"])} = 'values'))`;

let rows = 0, bytes = 0;
for (const [index, runId] of runs.entries()) {
  const [stats] = await store.rows<{ n: number; b: number | null }>(sql`SELECT COUNT(*) AS n, SUM(LENGTH(data)) AS b
    FROM events WHERE ${dropped(runId)}`);
  rows += Number(stats!.n);
  bytes += Number(stats!.b ?? 0);
  if (!dryRun) {
    await store.transaction([
      sql`DELETE FROM events WHERE ${dropped(runId)}`,
      // The final state is the thread state; the run row and end event no longer copy it.
      sql`UPDATE runs SET output = NULL WHERE id = ${runId}`,
      sql`UPDATE checkpoints SET state_values = '{}' WHERE run_id = ${runId}`,
      pg ? sql`UPDATE events SET data = (data::jsonb - 'output')::text WHERE run_id = ${runId} AND event = 'end'`
        : sql`UPDATE events SET data = json_remove(data, '$.output') WHERE run_id = ${runId} AND event = 'end'`,
    ]);
  }
  if ((index + 1) % 10 === 0) console.log(`${index + 1}/${runs.length} runs`);
}
const [left] = await store.rows<{ n: number; b: number | null }>(sql`SELECT COUNT(*) AS n, SUM(LENGTH(data)) AS b FROM events`);
console.log(`${dryRun ? "Would remove" : "Removed"} ${rows} events, ${Math.round(bytes / 1048576)} MB;`,
  `${dryRun ? "events now" : "events left"}: ${left!.n} rows, ${Math.round(Number(left!.b ?? 0) / 1048576)} MB`);
await store.close();
