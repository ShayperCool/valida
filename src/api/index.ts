import { Hono } from "hono";
import type { ApiRequestContext, JsonRecord, PlatformAdapter, StreamEvent } from "./types";
import { ApiError } from "./types";
import { LegacyV2Bridge } from "./v2";
import { currentAuthorization } from "../auth.ts";

type ApiEnv = { Variables: { principal: unknown } };

function json(data: unknown, status = 200, headers?: Record<string, string>): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "content-type": "application/json; charset=utf-8", ...headers },
  });
}

function error(status: number, detail: string): Response {
  return json({ detail }, status);
}

function requiredString(value: unknown, field: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new ApiError(422, `${field} is required`);
  }
  return value;
}

function record(value: unknown): JsonRecord {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new ApiError(422, "Expected a JSON object");
  }
  return value as JsonRecord;
}

async function body(request: Request): Promise<JsonRecord> {
  try {
    const parsed = record(await request.json());
    return currentAuthorization.getStore()?.payload ?? parsed;
  } catch (cause) {
    if (cause instanceof ApiError) throw cause;
    throw new ApiError(400, "Invalid JSON body");
  }
}

function query(request: Request): JsonRecord {
  return Object.fromEntries(new URL(request.url).searchParams);
}

function sse(events: AsyncIterable<StreamEvent>, headers?: Record<string, string>): Response {
  const encoder = new TextEncoder();
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      void (async () => {
        try {
        for await (const chunk of events) {
          if (!chunk || typeof chunk.event !== "string") continue;
          const lines = [
            ...(chunk.id === undefined ? [] : [`id: ${chunk.id.replaceAll(/[\r\n]/g, "")}`]),
            `event: ${chunk.event.replaceAll(/[\r\n]/g, "")}`,
            `data: ${JSON.stringify(chunk.data)}`,
            "",
            "",
          ];
          controller.enqueue(encoder.encode(lines.join("\n")));
        }
        controller.close();
        } catch (cause) {
        // A streaming response has already sent its status. Report execution errors as SSE.
        try {
          controller.enqueue(
            encoder.encode(`event: error\ndata: ${JSON.stringify({ error: "StreamError", message: String(cause) })}\n\n`),
          );
          controller.close();
        } catch {
          // The client disconnected.
        }
        }
      })();
    },
  });
  return new Response(stream, {
    headers: {
      "content-type": "text/event-stream; charset=utf-8",
      "cache-control": "no-cache, no-transform",
      connection: "keep-alive",
      "x-accel-buffering": "no",
      ...headers,
    },
  });
}

function validateRunPayload(payload: JsonRecord, threadId: string | null): void {
  requiredString(payload.assistant_id, "assistant_id");
  const hasInput = payload.input !== undefined && payload.input !== null;
  const hasCommand = payload.command !== undefined && payload.command !== null;
  if (hasInput && hasCommand && Object.keys(record(payload.input)).length > 0) {
    throw new ApiError(422, "Cannot specify both input and command");
  }
  if (hasInput && hasCommand) delete payload.input;
  if (threadId === null && !hasInput && !hasCommand && payload.checkpoint == null && payload.checkpoint_id == null) {
    throw new ApiError(422, "Must specify input, command, or checkpoint");
  }
}

function runHeaders(threadId: string | null, runId: string): Record<string, string> {
  const base = threadId === null ? `/runs/${runId}` : `/threads/${threadId}/runs/${runId}`;
  return { Location: `${base}/stream`, "Content-Location": base };
}

function notFound(kind: string, id: string): Response {
  return error(404, `${kind} '${id}' not found`);
}

/** Mount this Hono app behind custom middleware to provide authentication and HTTP extensions. */
export function createApi(adapter: PlatformAdapter): Hono<ApiEnv> {
  const app = new Hono<ApiEnv>();
  const v2 = adapter.v2 ?? new LegacyV2Bridge(adapter);
  const context = (request: Request, principal?: unknown): ApiRequestContext => ({ request, principal });
  const ctx = (c: { req: { raw: Request }; get: (key: "principal") => unknown }) =>
    context(c.req.raw, c.get("principal"));

  app.onError((cause) => {
    if (cause instanceof ApiError) return error(cause.status, cause.message);
    console.error(cause);
    return error(500, "Internal server error");
  });

  app.get("/live", () => json({ status: "alive" }));
  app.get("/ready", async (c) => {
    const health = (await adapter.health?.(ctx(c))) ?? { status: "healthy" };
    return health.status === "unhealthy" ? json(health, 503) : json({ status: "ready" });
  });
  app.get("/health", async (c) => {
    const health = (await adapter.health?.(ctx(c))) ?? {
        status: "healthy",
        database: "unknown",
        langgraph_checkpointer: "unknown",
        langgraph_store: "unknown",
      };
    return json(health, health.status === "unhealthy" ? 503 : 200);
  });
  app.get("/info", async (c) =>
    json(
      (await adapter.info?.(ctx(c))) ?? {
        name: "Valida",
        version: "0.1.0",
        description: "Self-hosted Agent Protocol server",
        status: "running",
        flags: { assistants: true, v2_event_streaming: true },
      },
    ),
  );

  app.post("/assistants", async (c) => {
    const payload = await body(c.req.raw);
    requiredString(payload.graph_id, "graph_id");
    return json(await adapter.assistants.create(payload, ctx(c)));
  });
  app.get("/assistants", async (c) => {
    const assistants = await adapter.assistants.search({ limit: 1_000_000, offset: 0 }, ctx(c));
    return json({ assistants, total: assistants.length });
  });
  app.post("/assistants/search", async (c) => {
    const payload = await body(c.req.raw);
    const assistants = await adapter.assistants.search(payload, ctx(c));
    const limit = Number(payload.limit ?? 10);
    const offset = Number(payload.offset ?? 0);
    const next = assistants.length === limit ? String(offset + limit) : "";
    return json(assistants, 200, next ? { "X-Pagination-Next": next } : undefined);
  });
  app.post("/assistants/count", async (c) => {
    const payload = await body(c.req.raw);
    if (adapter.assistants.count) return json(await adapter.assistants.count(payload, ctx(c)));
    const assistants = await adapter.assistants.search({ ...payload, limit: 1_000_000, offset: 0 }, ctx(c));
    return json(assistants.length);
  });
  app.post("/assistants/:assistantId/versions", async (c) => {
    if (!adapter.assistants.versions) return error(501, "Assistant versions are not implemented");
    const filters = c.req.raw.body === null ? {} : await body(c.req.raw);
    return json(await adapter.assistants.versions(c.req.param("assistantId"), filters, ctx(c)));
  });
  app.post("/assistants/:assistantId/latest", async (c) => {
    if (!adapter.assistants.setLatest) return error(501, "Assistant versions are not implemented");
    const id = c.req.param("assistantId");
    const version = Number(c.req.query("version") ?? (c.req.raw.body === null ? undefined : (await body(c.req.raw)).version));
    if (!Number.isInteger(version) || version < 1) throw new ApiError(422, "version must be a positive integer");
    const assistant = await adapter.assistants.setLatest(id, version, ctx(c));
    return assistant ? json(assistant) : notFound("Assistant", id);
  });
  app.get("/assistants/:assistantId/schemas", async (c) => {
    if (!adapter.assistants.schemas) return notFound("Assistant", c.req.param("assistantId"));
    const value = await adapter.assistants.schemas(c.req.param("assistantId"), ctx(c));
    return value ? json(value) : notFound("Assistant", c.req.param("assistantId"));
  });
  app.get("/assistants/:assistantId/graph", async (c) => {
    if (!adapter.assistants.graph) return notFound("Assistant", c.req.param("assistantId"));
    const value = await adapter.assistants.graph(c.req.param("assistantId"), query(c.req.raw), ctx(c));
    return value ? json(value) : notFound("Assistant", c.req.param("assistantId"));
  });
  app.get("/assistants/:assistantId/subgraphs", async (c) => {
    if (!adapter.assistants.subgraphs) return notFound("Assistant", c.req.param("assistantId"));
    const value = await adapter.assistants.subgraphs(c.req.param("assistantId"), query(c.req.raw), ctx(c));
    return value ? json(value) : notFound("Assistant", c.req.param("assistantId"));
  });
  app.get("/assistants/:assistantId/subgraphs/:namespace", async (c) => {
    if (!adapter.assistants.subgraphs) return notFound("Assistant", c.req.param("assistantId"));
    const value = await adapter.assistants.subgraphs(
      c.req.param("assistantId"),
      { ...query(c.req.raw), namespace: c.req.param("namespace") },
      ctx(c),
    );
    return value ? json(value) : notFound("Assistant", c.req.param("assistantId"));
  });
  app.get("/assistants/:assistantId", async (c) => {
    const id = c.req.param("assistantId");
    const assistant = await adapter.assistants.get(id, ctx(c));
    return assistant ? json(assistant) : notFound("Assistant", id);
  });
  app.patch("/assistants/:assistantId", async (c) => {
    const id = c.req.param("assistantId");
    const assistant = await adapter.assistants.update(id, await body(c.req.raw), ctx(c));
    return assistant ? json(assistant) : notFound("Assistant", id);
  });
  app.delete("/assistants/:assistantId", async (c) =>
    (await adapter.assistants.delete(c.req.param("assistantId"), ctx(c)))
      ? new Response(null, { status: 204 })
      : notFound("Assistant", c.req.param("assistantId")),
  );

  app.post("/threads", async (c) => json(await adapter.threads.create(await body(c.req.raw), ctx(c))));
  app.get("/threads", async (c) => {
    const threads = await adapter.threads.search({ ...query(c.req.raw), limit: 1_000_000, offset: 0 }, ctx(c));
    return json({ threads, total: threads.length });
  });
  app.post("/threads/search", async (c) => json(await adapter.threads.search(await body(c.req.raw), ctx(c))));
  app.post("/threads/count", async (c) => {
    const payload = await body(c.req.raw);
    if (adapter.threads.count) return json(await adapter.threads.count(payload, ctx(c)));
    return json((await adapter.threads.search({ ...payload, limit: 1_000_000, offset: 0 }, ctx(c))).length);
  });
  app.post("/threads/prune", async (c) => {
    if (!adapter.threads.prune) return error(501, "Thread pruning is not implemented");
    return json(await adapter.threads.prune(await body(c.req.raw), ctx(c)));
  });
  app.post("/threads/:threadId/copy", async (c) => {
    const id = c.req.param("threadId");
    if (!adapter.threads.copy) return error(501, "Thread copying is not implemented");
    const thread = await adapter.threads.copy(id, ctx(c));
    return thread ? json(thread) : notFound("Thread", id);
  });
  app.get("/threads/:threadId/state", async (c) => {
    const id = c.req.param("threadId");
    const state = await adapter.threads.getState(id, null, ctx(c));
    return state ? json(state) : notFound("Thread", id);
  });
  app.get("/threads/:threadId/state/:checkpointId", async (c) => {
    const id = c.req.param("threadId");
    const state = await adapter.threads.getState(id, c.req.param("checkpointId"), ctx(c));
    return state ? json(state) : notFound("Checkpoint", c.req.param("checkpointId"));
  });
  app.post("/threads/:threadId/state/checkpoint", async (c) => {
    const id = c.req.param("threadId");
    const payload = await body(c.req.raw);
    const state = await adapter.threads.getState(id, record(payload.checkpoint), ctx(c));
    return state ? json(state) : notFound("Checkpoint", String(record(payload.checkpoint).checkpoint_id));
  });
  app.post("/threads/:threadId/state", async (c) => {
    const id = c.req.param("threadId");
    const result = await adapter.threads.updateState(id, await body(c.req.raw), ctx(c));
    if (!result) return notFound("Thread", id);
    if (result.configurable) return json(result);
    const checkpoint = result.checkpoint && typeof result.checkpoint === "object" ? result.checkpoint : result;
    return json({ ...result, configurable: { thread_id: id, ...(checkpoint as JsonRecord) } });
  });
  app.post("/threads/:threadId/history", async (c) => {
    const id = c.req.param("threadId");
    const history = await adapter.threads.history(id, await body(c.req.raw), ctx(c));
    return history ? json(history) : notFound("Thread", id);
  });
  app.get("/threads/:threadId/history", async (c) => {
    const id = c.req.param("threadId");
    const history = await adapter.threads.history(id, query(c.req.raw), ctx(c));
    return history ? json(history) : notFound("Thread", id);
  });
  app.get("/threads/:threadId", async (c) => {
    const id = c.req.param("threadId");
    const thread = await adapter.threads.get(id, ctx(c));
    return thread ? json(thread) : notFound("Thread", id);
  });
  app.patch("/threads/:threadId", async (c) => {
    const id = c.req.param("threadId");
    const thread = await adapter.threads.update(id, await body(c.req.raw), ctx(c));
    if (!thread) return notFound("Thread", id);
    return c.req.header("Prefer") === "return=minimal" ? new Response(null, { status: 204 }) : json(thread);
  });
  app.delete("/threads/:threadId", async (c) =>
    (await adapter.threads.delete(c.req.param("threadId"), ctx(c)))
      ? new Response(null, { status: 204 })
      : notFound("Thread", c.req.param("threadId")),
  );

  async function createRun(request: Request, threadId: string | null, requestContext: ApiRequestContext) {
    const payload = await body(request);
    validateRunPayload(payload, threadId);
    return adapter.runs.create(threadId, payload, requestContext);
  }

  for (const prefix of ["/runs", "/threads/:threadId/runs"] as const) {
    const threadId = (c: { req: { param: (name: string) => string } }) =>
      prefix === "/runs" ? null : c.req.param("threadId");

    app.post(`${prefix}/stream`, async (c) => {
      const run = await createRun(c.req.raw, threadId(c), ctx(c));
      return sse(adapter.runs.events(threadId(c), run.run_id, null, ctx(c)), runHeaders(threadId(c), run.run_id));
    });
    app.post(`${prefix}/wait`, async (c) => {
      const run = await createRun(c.req.raw, threadId(c), ctx(c));
      return json(await adapter.runs.join(threadId(c), run.run_id, ctx(c)), 200, runHeaders(threadId(c), run.run_id));
    });
    app.post(prefix, async (c) => {
      const run = await createRun(c.req.raw, threadId(c), ctx(c));
      return json(run, 200, runHeaders(threadId(c), run.run_id));
    });
    app.get(`${prefix}/:runId/stream`, async (c) => {
      const id = c.req.param("runId");
      const run = await adapter.runs.get(threadId(c), id, ctx(c));
      if (!run) return notFound("Run", id);
      return sse(
        adapter.runs.events(threadId(c), id, c.req.header("Last-Event-ID") ?? null, ctx(c)),
        runHeaders(threadId(c), id),
      );
    });
    app.get(`${prefix}/:runId/join`, async (c) => {
      const id = c.req.param("runId");
      const run = await adapter.runs.get(threadId(c), id, ctx(c));
      return run ? json(await adapter.runs.join(threadId(c), id, ctx(c))) : notFound("Run", id);
    });
    app.get(`${prefix}/:runId`, async (c) => {
      const id = c.req.param("runId");
      const run = await adapter.runs.get(threadId(c), id, ctx(c));
      return run ? json(run) : notFound("Run", id);
    });
  }

  app.get("/threads/:threadId/runs", async (c) =>
    json(await adapter.runs.list(c.req.param("threadId"), query(c.req.raw), ctx(c))),
  );
  app.post("/threads/:threadId/runs/:runId/cancel", async (c) => {
    const { threadId, runId } = c.req.param();
    const action = c.req.query("action") === "rollback" ? "rollback" : "interrupt";
    return (await adapter.runs.cancel(threadId, runId, action, ctx(c)))
      ? new Response(null, { status: 204 })
      : notFound("Run", runId);
  });
  app.patch("/threads/:threadId/runs/:runId", async (c) => {
    const { threadId, runId } = c.req.param();
    if (!adapter.runs.update) return error(501, "Run updating is not implemented");
    const run = await adapter.runs.update(threadId, runId, await body(c.req.raw), ctx(c));
    return run ? json(run) : notFound("Run", runId);
  });
  app.delete("/threads/:threadId/runs/:runId", async (c) => {
    const { threadId, runId } = c.req.param();
    if (!adapter.runs.delete) return error(501, "Run deletion is not implemented");
    return (await adapter.runs.delete(threadId, runId, ctx(c)))
      ? new Response(null, { status: 204 })
      : notFound("Run", runId);
  });

  app.post("/threads/:threadId/commands", async (c) => {
    return json(await v2.command(c.req.param("threadId"), await body(c.req.raw), ctx(c)));
  });
  app.post("/threads/:threadId/stream/events", async (c) => {
    const payload = await body(c.req.raw);
    if (!Array.isArray(payload.channels) || payload.channels.length === 0) {
      throw new ApiError(400, "channels must be a non-empty array");
    }
    return sse(v2.events(c.req.param("threadId"), payload, ctx(c)));
  });

  app.put("/store/items", async (c) => {
    if (!adapter.store) return error(501, "Store is not configured");
    const payload = await body(c.req.raw);
    if (!Array.isArray(payload.namespace) || !payload.namespace.every((part) => typeof part === "string")) {
      throw new ApiError(422, "namespace must be an array of strings");
    }
    requiredString(payload.key, "key");
    record(payload.value);
    await adapter.store.put(payload, ctx(c));
    return new Response(null, { status: 204 });
  });
  app.get("/store/items", async (c) => {
    if (!adapter.store) return error(501, "Store is not configured");
    const key = requiredString(c.req.query("key"), "key");
    const repeated = new URL(c.req.raw.url).searchParams.getAll("namespace");
    const namespace = repeated.length === 1 ? repeated[0]!.split(".") : repeated;
    const item = await adapter.store.get(namespace, key, ctx(c));
    return item ? json(item) : notFound("Item", key);
  });
  app.delete("/store/items", async (c) => {
    if (!adapter.store) return error(501, "Store is not configured");
    const payload = c.req.header("Content-Type")?.includes("application/json")
      ? await body(c.req.raw)
      : query(c.req.raw);
    const namespace = Array.isArray(payload.namespace)
      ? payload.namespace
      : typeof payload.namespace === "string"
        ? payload.namespace.split(".")
        : [];
    if (!namespace.every((part) => typeof part === "string")) {
      throw new ApiError(422, "namespace must be an array of strings");
    }
    await adapter.store.delete(namespace as string[], requiredString(payload.key, "key"), ctx(c));
    return new Response(null, { status: 204 });
  });
  app.post("/store/items/search", async (c) => {
    if (!adapter.store) return error(501, "Store is not configured");
    return json(await adapter.store.search(await body(c.req.raw), ctx(c)));
  });
  app.post("/store/namespaces", async (c) => {
    if (!adapter.store) return error(501, "Store is not configured");
    return json(await adapter.store.namespaces(await body(c.req.raw), ctx(c)));
  });

  app.post("/runs/crons", async (c) => {
    if (!adapter.crons) return error(501, "Crons are not configured");
    const payload = await body(c.req.raw);
    requiredString(payload.assistant_id, "assistant_id");
    requiredString(payload.schedule, "schedule");
    return json(await adapter.crons.create(null, payload, ctx(c)));
  });
  app.post("/threads/:threadId/runs/crons", async (c) => {
    if (!adapter.crons) return error(501, "Crons are not configured");
    const payload = await body(c.req.raw);
    requiredString(payload.assistant_id, "assistant_id");
    requiredString(payload.schedule, "schedule");
    return json(await adapter.crons.create(c.req.param("threadId"), payload, ctx(c)));
  });
  app.post("/runs/crons/search", async (c) =>
    adapter.crons ? json(await adapter.crons.search(await body(c.req.raw), ctx(c))) : error(501, "Crons are not configured"),
  );
  app.post("/runs/crons/count", async (c) =>
    adapter.crons ? json(await adapter.crons.count(await body(c.req.raw), ctx(c))) : error(501, "Crons are not configured"),
  );
  app.patch("/runs/crons/:cronId", async (c) => {
    if (!adapter.crons) return error(501, "Crons are not configured");
    const cron = await adapter.crons.update(c.req.param("cronId"), await body(c.req.raw), ctx(c));
    return cron ? json(cron) : notFound("Cron", c.req.param("cronId"));
  });
  app.delete("/runs/crons/:cronId", async (c) => {
    if (!adapter.crons) return error(501, "Crons are not configured");
    return (await adapter.crons.delete(c.req.param("cronId"), ctx(c)))
      ? new Response(null, { status: 204 })
      : notFound("Cron", c.req.param("cronId"));
  });

  return app;
}

export type { PlatformAdapter, ApiRequestContext, StreamEvent } from "./types";
export { ApiError } from "./types";
