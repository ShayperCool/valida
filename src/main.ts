import { Hono } from "hono";
import { cors } from "hono/cors";
import { createApi } from "./api/index.ts";
import { authMiddleware, loadAuth } from "./auth.ts";
import { bootstrap } from "./bootstrap.ts";
import { createPlatformAdapter } from "./platform.ts";
import { createStoreExtension } from "./extensions/store.ts";
import { createCronExtension } from "./extensions/crons.ts";
import { loadCustomApp, loadMiddleware } from "./plugins.ts";

const { runtime, config, mode } = await bootstrap();
const app = new Hono();
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

const kv = await createStoreExtension(runtime.store);
const crons = await createCronExtension(runtime.store, runtime);
const adapter = createPlatformAdapter(runtime, runtime.store, runtime.listGraphs(), { store: kv, crons });
app.route("/", createApi(adapter));
const custom = await loadCustomApp(config);
if (custom) app.route("/", custom);

if (mode === "distributed" && process.env.RUN_WORKER_IN_API !== "false") runtime.startWorker();
crons.start();
const port = Number(process.env.PORT ?? 2026);
const hostname = process.env.HOST ?? "127.0.0.1";
const server = Bun.serve({ port, hostname, fetch: app.fetch });
console.info(`Valida listening on http://${hostname}:${server.port} (${mode})`);
const shutdown = async () => { server.stop(); crons.stop(); await runtime.close(); };
process.once("SIGINT", () => { void shutdown(); });
process.once("SIGTERM", () => { void shutdown(); });
