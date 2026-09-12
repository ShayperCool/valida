import { expect, test } from "bun:test";
import { Hono } from "hono";
import { createApi, type PlatformAdapter } from "./api/index.ts";
import { authMiddleware, type AuthProvider } from "./auth.ts";
import { createRuntime } from "./engine/index.ts";
import { createStoreExtension } from "./extensions/store.ts";

test("store authorization filters item reads, search, namespaces and delete", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite", url: ":memory:" } });
  try {
    const store = await createStoreExtension(runtime.store);
    const context = { request: new Request("http://valida.test") };
    for (const name of ["alice", "bob"]) {
      await store.put({ namespace: ["team", name], key: "profile", value: { name } }, context);
    }
    const provider: AuthProvider = {
      authenticate(request) { return { identity: request.headers.get("x-user") ?? "" }; },
      authorize(auth) {
        return auth.resource === "store" && ["read", "search", "delete"].includes(auth.action)
          ? { namespace: ["team", auth.user.identity] } : true;
      },
    };
    const app = new Hono();
    app.use("*", authMiddleware(provider));
    app.route("/", createApi({ store } as PlatformAdapter));
    const headers = { "x-user": "alice", "content-type": "application/json" };

    const own = await app.request("/store/items?namespace=team.alice&key=profile", { headers });
    expect(own.status).toBe(200);
    const other = await app.request("/store/items?namespace=team.bob&key=profile", { headers });
    expect(other.status).toBe(404);
    const search = await app.request("/store/items/search", {
      method: "POST", headers, body: JSON.stringify({ namespace_prefix: ["team"] }),
    });
    expect((await search.json() as { total: number }).total).toBe(1);
    const namespaces = await app.request("/store/namespaces", {
      method: "POST", headers, body: JSON.stringify({ prefix: ["team"] }),
    });
    expect((await namespaces.json() as { namespaces: string[][] }).namespaces).toEqual([["team", "alice"]]);
    const deniedDelete = await app.request("/store/items?namespace=team.bob&key=profile", {
      method: "DELETE", headers: { "x-user": "alice" },
    });
    expect(deniedDelete.status).toBe(403);
    expect(await store.get(["team", "bob"], "profile", context)).not.toBeNull();
  } finally {
    await runtime.close();
  }
});
