import { mkdir } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import type { DatabaseConfig } from "./db/index.ts";
import { createRuntime, type CompiledGraphLike, type GraphDefinition } from "./engine/index.ts";
import { loadConfig, loadModuleRef } from "./config.ts";
import { seedDefaultAssistants } from "./platform.ts";

export async function bootstrap(options: { workerOnly?: boolean } = {}) {
  const config = await loadConfig();
  const url = process.env.DATABASE_URL ?? "file:./data/valida.db";
  let db: DatabaseConfig;
  if (url.startsWith("postgres://") || url.startsWith("postgresql://")) {
    db = { dialect: "postgres", url };
  } else {
    const path = url.replace(/^file:/, "");
    if (path !== ":memory:") await mkdir(dirname(resolve(path)), { recursive: true });
    db = { dialect: "sqlite", url: path };
  }
  const mode = process.env.EXECUTION_MODE ?? config.value.execution?.mode ?? "standalone";
  const redisUrl = process.env.REDIS_URL;
  if (mode === "distributed" && (!redisUrl || db.dialect !== "postgres")) {
    throw new Error("Distributed mode requires PostgreSQL DATABASE_URL and REDIS_URL");
  }
  const runtime = await createRuntime({
    db,
    queue: mode === "distributed" ? { redisUrl: redisUrl!, concurrency: config.value.execution?.concurrency } : undefined,
    inline: mode !== "distributed",
  });
  for (const [id, ref] of Object.entries(config.value.graphs)) {
    const graph = await loadModuleRef<CompiledGraphLike | Omit<GraphDefinition, "id"> | ((checkpointer: unknown) => CompiledGraphLike)>(ref, config.directory);
    if (graph && typeof graph === "object" && "nodes" in graph && "entrypoint" in graph) {
      runtime.registerGraph({ ...graph, id } as GraphDefinition);
    } else {
      runtime.registerGraph({ id, graph: graph as CompiledGraphLike });
    }
  }
  if (!options.workerOnly) await seedDefaultAssistants(runtime.store, runtime.listGraphs());
  return { runtime, config, mode };
}
