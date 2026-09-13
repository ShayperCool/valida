import { Hono } from "hono";
import { cors } from "hono/cors";
import { createApi } from "./api/index.ts";
import { NativeV2Bridge } from "./api/native_v2.ts";
import { V1StreamBridge } from "./api/v1_stream.ts";
import { createAgUiApi } from "./api/ag_ui.ts";
import { authMiddleware, loadAuth } from "./auth.ts";
import { bootstrap } from "./bootstrap.ts";
import { createPlatformAdapter } from "./platform.ts";
import { createStoreExtension } from "./extensions/store.ts";
import { loadStoreIndex } from "./extensions/store_config.ts";
import { createCronExtension } from "./extensions/crons.ts";
import { createAssistantVersionsExtension } from "./extensions/assistant_versions.ts";
import { ThreadPruner } from "./extensions/thread_prune.ts";
import { resolveThreadTtlPolicy } from "./extensions/thread_ttl_config.ts";
import { loadCustomApp, loadMiddleware } from "./plugins.ts";

const { runtime, config, mode, telemetry } = await bootstrap();
const ttlPolicy = resolveThreadTtlPolicy(config.value.checkpointer);
const app = new Hono();
if (telemetry) app.use("*", telemetry.middleware());
const corsOptions = config.value.http?.cors;
const origins = corsOptions?.allow_origins ?? ["*"];
app.use("*", cors({
  origin: origins.includes("*") ? "*" : origins,
  credentials: corsOptions?.allow_credentials ?? !origins.includes("*"),
  allowHeaders: ["Content-Type", "Authorization", "X-Api-Key", "Last-Event-ID"],
  exposeHeaders: ["X-Pagination-Next", "Content-Location", "Location"],
}));

const auth = authMiddleware(await loadAuth(config), {
  protectCustomRoutes: config.value.http?.enable_custom_route_auth,
});
const middleware = await loadMiddleware(config);
if (config.value.http?.middleware_order === "auth_first") {
  app.use("*", auth);
  for (const handler of middleware) app.use("*", handler);
} else {
  for (const handler of middleware) app.use("*", handler);
  app.use("*", auth);
}

const index = await loadStoreIndex(config.value.store?.index, config.directory);
const kv = await createStoreExtension(runtime.store, { index });
const crons = await createCronExtension(runtime.store, runtime);
const versions = await createAssistantVersionsExtension(runtime.store);
const pruner = new ThreadPruner(runtime.store, { ttlSweepLimit: ttlPolicy?.sweepLimit });
const adapter = createPlatformAdapter(runtime, runtime.store, runtime.listGraphs(), {
  store: kv, crons, versions, pruner, ttl: ttlPolicy,
});
adapter.v2 = new NativeV2Bridge(adapter, runtime);
adapter.v1 = new V1StreamBridge(adapter, runtime);
app.route("/", createApi(adapter));
app.route("/", createAgUiApi(adapter, runtime));
const custom = await loadCustomApp(config);
if (custom) app.route("/", custom);

if (mode === "distributed" && process.env.RUN_WORKER_IN_API !== "false") runtime.startWorker();
crons.start();
pruner.start(ttlPolicy?.sweepIntervalMs);
const port = Number(process.env.PORT ?? 2026);
const hostname = process.env.HOST ?? "127.0.0.1";
const server = Bun.serve({ port, hostname, fetch: app.fetch });
console.info(`Valida listening on http://${hostname}:${server.port} (${mode})`);
const shutdown = async () => { server.stop(); crons.stop(); pruner.stop(); await runtime.close(); await telemetry?.shutdown(); };
process.once("SIGINT", () => { void shutdown(); });
process.once("SIGTERM", () => { void shutdown(); });
