import { describe, expect, test } from "bun:test";
import { Hono } from "hono";
import { authMiddleware, currentUser, type AuthProvider } from "./auth.ts";

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
    const response = await app.request("/threads/1");
    expect(response.status).toBe(401);
  });

  test("passes authenticated identity to request work", async () => {
    const response = await app.request("/threads/1", { headers: { authorization: "Bearer valid" } });
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ user: "alice" });
  });

  test("applies authorization and keeps health public", async () => {
    const forbidden = await app.request("/threads/1", {
      method: "DELETE", headers: { authorization: "Bearer valid" },
    });
    expect(forbidden.status).toBe(403);
    expect((await app.request("/health")).status).toBe(200);
  });
});
