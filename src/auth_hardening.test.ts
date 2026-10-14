import { expect, test } from "bun:test";
import { Hono } from "hono";
import { authMiddleware, type AuthProvider } from "./auth.ts";
import { createApi } from "./api/index.ts";
import { createRuntime } from "./engine/index.ts";
import { createStoreExtension } from "./extensions/store.ts";
import { createPlatformAdapter, seedDefaultAssistants } from "./platform.ts";

const provider: AuthProvider = {
  authenticate(request) {
    const token = request.headers.get("authorization");
    if (token !== "Bearer alice" && token !== "Bearer bob") throw new Error("Invalid token");
    return { identity: token.slice("Bearer ".length) };
  },
  authorize(context, value) {
    if (context.resource === "threads" && context.action === "create") {
      return { ...value, metadata: { ...(value.metadata as object | undefined), tenant: "forced" } };
    }
    if (context.resource === "store" && context.action === "delete") {
      const namespace = typeof value.namespace === "string" ? value.namespace.split(".") : value.namespace;
      return (namespace as string[] | undefined)?.[0] === context.user.identity;
    }
    return true;
  },
};

async function harness() {
  const runtime = await createRuntime({ db: { dialect: "sqlite", url: ":memory:" } });
  runtime.registerGraph({ id: "echo", entrypoint: "reply", nodes: {
    reply: (_values, context) => ({ seen: context.config.configurable }),
  } });
  await seedDefaultAssistants(runtime.store, runtime.listGraphs());
  const adapter = createPlatformAdapter(runtime, runtime.store, runtime.listGraphs(), {
    store: await createStoreExtension(runtime.store),
  });
  const app = new Hono();
  app.use("*", authMiddleware(provider));
  app.route("/", createApi(adapter));
  const json = (identity: string) => ({ authorization: `Bearer ${identity}`, "content-type": "application/json" });
  const send = (path: string, method: string, identity: string | null, body?: unknown, headers?: Record<string, string>) =>
    app.request(`http://valida.test${path}`, { method,
      headers: { ...(identity ? json(identity) : {}), ...headers },
      body: body === undefined ? undefined : JSON.stringify(body) });
  return { runtime, send };
}

test("percent-encoded and HEAD requests cannot skip authentication", async () => {
  const { runtime, send } = await harness();
  try {
    const thread = await (await send("/threads", "POST", "alice", { metadata: { secret: "s3" } })).json() as { thread_id: string };
    const id = thread.thread_id;
    expect((await send(`/%74hreads/${id}`, "GET", null)).status).toBe(401);
    expect((await send("/%74hreads/search", "POST", null, {}, { "content-type": "application/json" })).status).toBe(401);
    expect((await send(`/thread%73/${id}`, "DELETE", null)).status).toBe(401);
    expect((await send(`/%73tore/items/search`, "POST", null, {}, { "content-type": "application/json" })).status).toBe(401);
    expect((await send(`/threads/${id}`, "HEAD", null)).status).toBe(401);
    expect((await send(`/threads/${id}`, "PUT", null)).status).toBe(401);
    // Ownership still applies on the encoded path once authenticated.
    expect((await send(`/%74hreads/${id}`, "GET", "bob")).status).toBe(403);
    expect((await send(`/%74hreads/${id}`, "GET", "alice")).status).toBe(200);
    expect((await send(`/threads/${id}`, "HEAD", "alice")).status).toBe(200);
    expect((await send("/health", "GET", null)).status).toBe(200);
  } finally { await runtime.close(); }
});

test("authorization sees JSON bodies sent with another content type", async () => {
  const { runtime, send } = await harness();
  try {
    const created = await send("/threads", "POST", "alice", { metadata: { tenant: "evil" } },
      { "content-type": "text/plain" });
    expect(((await created.json()) as { metadata: unknown }).metadata).toEqual({ tenant: "forced", _owner: "alice" });

    await send("/store/items", "PUT", "bob", { namespace: ["bob"], key: "k", value: { v: 1 } });
    // The body the handler acts on is the body that was authorized.
    const smuggled = await send("/store/items?namespace=alice&key=k", "DELETE", "alice",
      { namespace: ["bob"], key: "k" }, { "content-type": "text/plain" });
    expect(smuggled.status).toBe(403);
    expect((await send("/store/items?namespace=bob&key=k", "GET", "bob")).status).toBe(200);
    expect((await send("/store/items?namespace=bob&key=k", "DELETE", "alice")).status).toBe(403);
    expect((await send("/store/items?namespace=bob&key=k", "DELETE", "bob")).status).toBe(204);
  } finally { await runtime.close(); }
});

test("clients cannot reassign thread ownership or inject runtime metadata", async () => {
  const { runtime, send } = await harness();
  try {
    const thread = await (await send("/threads", "POST", "alice", { metadata: { _owner: "bob" } })).json() as
      { thread_id: string; metadata: Record<string, unknown> };
    expect(thread.metadata._owner).toBe("alice");
    const patched = await (await send(`/threads/${thread.thread_id}`, "PATCH", "alice",
      { metadata: { _owner: "bob", label: "x" } })).json() as { metadata: Record<string, unknown> };
    expect(patched.metadata).toMatchObject({ _owner: "alice", label: "x" });
    expect((await send(`/threads/${thread.thread_id}`, "GET", "bob")).status).toBe(403);

    expect((await send("/threads", "POST", "alice", { thread_id: thread.thread_id })).status).toBe(409);
    expect((await send("/threads", "POST", "bob", { thread_id: thread.thread_id, if_exists: "do_nothing" })).status).toBe(403);
    const same = await send("/threads", "POST", "alice", { thread_id: thread.thread_id, if_exists: "do_nothing" });
    expect(((await same.json()) as { thread_id: string }).thread_id).toBe(thread.thread_id);
    expect((await send("/assistants", "POST", "alice", { graph_id: "echo", assistant_id: "echo" })).status).toBe(409);

    const output = await (await send(`/threads/${thread.thread_id}/runs/wait`, "POST", "alice", {
      assistant_id: "echo", input: {},
      metadata: { __resumeProvided: true, __commandGoto: "missing", note: "kept" },
      config: { configurable: { __pregel_resuming: true, user_setting: 1 } },
    })).json() as { seen: Record<string, unknown> };
    expect(output.seen).toEqual({ user_setting: 1 });
    const runs = await (await send(`/threads/${thread.thread_id}/runs`, "GET", "alice")).json() as
      Array<{ run_id: string; metadata: unknown }>;
    expect(runs[0]!.metadata).toEqual({ note: "kept" });
    // Only the server's own reserved keys may appear; the client's were dropped.
    expect((await runtime.store.getRun(runs[0]!.run_id))!.metadata).toEqual({ note: "kept", __stream_modes: ["values"] });

    const copy = await (await send(`/threads/${thread.thread_id}/copy`, "POST", "alice")).json() as
      { metadata: Record<string, unknown> };
    expect(copy.metadata._owner).toBe("alice");
  } finally { await runtime.close(); }
});
