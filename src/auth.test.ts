import { describe, expect, test } from "bun:test";
import { Hono } from "hono";
import {
  authMiddleware, currentAuthorization, currentUser, routeAuthTarget, type AuthProvider,
} from "./auth.ts";

const credential = { authorization: "Bearer valid" };

describe("Agent Protocol authorization routes", () => {
  test.each([
    ["POST", "/assistants/search", "assistants", "search", {}],
    ["POST", "/assistants/agent/latest", "assistants", "update", { assistant_id: "agent" }],
    ["POST", "/threads/one/state/checkpoint", "threads", "read", { thread_id: "one" }],
    ["POST", "/threads/one/history", "threads", "read", { thread_id: "one" }],
    ["POST", "/threads/one/copy", "threads", "create", { thread_id: "one" }],
    ["POST", "/threads/one/runs/stream", "threads", "create_run", { thread_id: "one" }],
    ["POST", "/threads/one/runs/run-1/cancel", "threads", "update", { thread_id: "one", run_id: "run-1" }],
    ["DELETE", "/threads/one/runs/run-1", "threads", "delete", { thread_id: "one", run_id: "run-1" }],
    ["POST", "/runs/crons", "crons", "create", {}],
    ["POST", "/threads/one/runs/crons", "crons", "create", { thread_id: "one" }],
    ["POST", "/runs/crons/search", "crons", "search", {}],
    ["POST", "/runs/crons/count", "crons", "search", {}],
    ["PATCH", "/runs/crons/cron-1", "crons", "update", { cron_id: "cron-1" }],
    ["DELETE", "/runs/crons/cron-1", "crons", "delete", { cron_id: "cron-1" }],
    ["POST", "/store/items/search", "store", "search", {}],
  ])("maps %s %s to %s.%s", (method, path, resource, action, params) => {
    expect(routeAuthTarget(method, path)).toEqual({ resource, action, params });
  });
});

describe("custom auth middleware", () => {
  const provider: AuthProvider = {
    authenticate(request) {
      if (request.headers.get("authorization") !== "Bearer valid") throw new Error("Invalid token");
      return { identity: "alice", permissions: ["read"] };
    },
    authorize(context) {
      return context.action !== "delete";
    },
  };

  const app = new Hono();
  app.use("*", authMiddleware(provider));
  app.get("/health", c => c.json({ ok: true }));
  app.get("/threads/:id", c => c.json({ user: currentUser.getStore()?.identity }));
  app.delete("/threads/:id", c => c.json({ deleted: true }));

  test("rejects missing credentials", async () => {
    expect((await app.request("/threads/1")).status).toBe(401);
  });

  test("passes authenticated identity to request work", async () => {
    const response = await app.request("/threads/1", { headers: credential });
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ user: "alice" });
  });

  test("applies denial and keeps health public", async () => {
    const forbidden = await app.request("/threads/1", { method: "DELETE", headers: credential });
    expect(forbidden.status).toBe(403);
    expect((await app.request("/health")).status).toBe(200);
  });

  test("exposes the JSON payload and route context without consuming the route body", async () => {
    let seen: unknown;
    const local = new Hono();
    local.use("*", authMiddleware({
      authenticate: provider.authenticate,
      authorize(context, value) {
        seen = { context, value };
        return true;
      },
    }));
    local.post("/threads/:threadId/runs/stream", async c => c.json({
      body: await c.req.json(), state: currentAuthorization.getStore()?.payload,
    }));
    const response = await local.request("/threads/t-1/runs/stream?debug=1&debug=2", {
      method: "POST", headers: { ...credential, "content-type": "application/json" },
      body: JSON.stringify({ assistant_id: "echo", input: { value: 7 } }),
    });
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({
      body: { assistant_id: "echo", input: { value: 7 } },
      state: { assistant_id: "echo", input: { value: 7 } },
    });
    expect(seen).toMatchObject({
      context: {
        user: { identity: "alice" }, resource: "threads", action: "create_run",
        permissions: ["read"], method: "POST", path: "/threads/t-1/runs/stream",
        params: { thread_id: "t-1" }, query: { debug: ["1", "2"] },
      },
      value: { assistant_id: "echo", input: { value: 7 } },
    });
  });

  test("keeps returned read filters and write replacements available to handlers", async () => {
    const local = new Hono();
    local.use("*", authMiddleware({
      authenticate: provider.authenticate,
      authorize(context, value) {
        if (context.action === "search") return { owner: context.user.identity };
        value.metadata = { authorized: true };
        return { ...value, metadata: { authorized: true, version: 2 } };
      },
    }));
    local.post("/threads/search", c => c.json(currentAuthorization.getStore()));
    local.post("/threads", c => c.json(currentAuthorization.getStore()));
    const search = await local.request("/threads/search", {
      method: "POST", headers: { ...credential, "content-type": "application/json" },
      body: JSON.stringify({ status: "idle" }),
    });
    expect(await search.json()).toMatchObject({
      resource: "threads", action: "search", value: { status: "idle" },
      filter: { owner: "alice" }, payload: null,
    });
    const create = await local.request("/threads", {
      method: "POST", headers: { ...credential, "content-type": "application/json" },
      body: JSON.stringify({ metadata: { original: true } }),
    });
    expect(await create.json()).toMatchObject({
      action: "create", value: { metadata: { authorized: true } },
      payload: { metadata: { authorized: true, version: 2 } }, filter: null,
    });
  });

  test("captures in-place write modifications when authorize returns void", async () => {
    const local = new Hono();
    local.use("*", authMiddleware({
      authenticate: provider.authenticate,
      authorize(_context, value) { value.owner = "alice"; },
    }));
    local.patch("/threads/t-1", c => c.json(currentAuthorization.getStore()?.payload));
    const response = await local.request("/threads/t-1", {
      method: "PATCH", headers: { ...credential, "content-type": "application/json" }, body: "{}",
    });
    expect(await response.json()).toEqual({ owner: "alice" });
  });

  test("does not invent a JSON payload for a bodyless write", async () => {
    const local = new Hono();
    local.use("*", authMiddleware({
      authenticate: provider.authenticate,
      authorize() { return { metadata: { authorized: true } }; },
    }));
    local.post("/threads/:threadId/copy", c => c.json({
      payload: currentAuthorization.getStore()?.payload,
      value: currentAuthorization.getStore()?.value,
    }));
    const response = await local.request("/threads/t-1/copy", {
      method: "POST", headers: credential,
    });
    expect(await response.json()).toEqual({ payload: null, value: { thread_id: "t-1" } });
  });

  test("does not leak authorization data between concurrent requests", async () => {
    const local = new Hono();
    local.use("*", authMiddleware({
      authenticate(request) { return { identity: request.headers.get("authorization") ?? "" }; },
      authorize(context) { return { owner: context.user.identity }; },
    }));
    local.post("/threads/search", async c => {
      await new Promise(resolve => setTimeout(resolve, 5));
      return c.json({ identity: currentUser.getStore()?.identity, filter: currentAuthorization.getStore()?.filter });
    });
    const [a, b] = await Promise.all([
      local.request("/threads/search", { method: "POST", headers: { authorization: "a" } }),
      local.request("/threads/search", { method: "POST", headers: { authorization: "b" } }),
    ]);
    expect(await a.json()).toEqual({ identity: "a", filter: { owner: "a" } });
    expect(await b.json()).toEqual({ identity: "b", filter: { owner: "b" } });
  });

  test("custom routes remain optional to protect, while protected routes authenticate", async () => {
    const open = new Hono();
    open.use("*", authMiddleware(provider));
    open.get("/custom", c => c.json({ ok: true }));
    expect((await open.request("/custom")).status).toBe(200);

    const protectedApp = new Hono();
    protectedApp.use("*", authMiddleware(provider, { protectCustomRoutes: true }));
    protectedApp.get("/custom", c => c.json({ user: currentAuthorization.getStore()?.user.identity }));
    protectedApp.delete("/custom", c => c.json({ deleted: true }));
    expect((await protectedApp.request("/custom")).status).toBe(401);
    const authorized = await protectedApp.request("/custom", { headers: credential });
    expect(await authorized.json()).toEqual({ user: "alice" });
    expect((await protectedApp.request("/custom", {
      method: "DELETE", headers: credential,
    })).status).toBe(403);
  });
});
