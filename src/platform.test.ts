import { expect, test } from "bun:test";
import { Client } from "@langchain/langgraph-sdk";
import { createApi } from "./api/index.ts";
import { createRuntime } from "./engine/index.ts";
import { createAssistantVersionsExtension } from "./extensions/assistant_versions.ts";
import { createPlatformAdapter, seedDefaultAssistants } from "./platform.ts";
import { Hono } from "hono";
import { authMiddleware, type AuthProvider } from "./auth.ts";

test("platform adapter records assistant context and versions through the SDK", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite", url: ":memory:" } });
  try {
    runtime.registerGraph({ id: "echo", entrypoint: "reply", nodes: {
      reply: (_values, context) => ({ text: (context.config.configurable as { text?: string })?.text }),
    } });
    await seedDefaultAssistants(runtime.store, runtime.listGraphs());
    const versions = await createAssistantVersionsExtension(runtime.store);
    const app = createApi(createPlatformAdapter(runtime, runtime.store, runtime.listGraphs(), { versions }));
    const client = new Client({ apiUrl: "http://valida.test", apiKey: null,
      callerOptions: { maxRetries: 0,
        fetch: (input: RequestInfo | URL, init?: RequestInit) => app.fetch(new Request(input, init)) } });

    const created = await client.assistants.create({ graphId: "echo", name: "test",
      config: { configurable: { text: "first" } }, context: { tenant: "example" } });
    expect(created.version).toBe(1);
    expect(created.context).toEqual({ tenant: "example" });
    const firstThread = await client.threads.create();
    expect(await client.runs.wait(firstThread.thread_id, created.assistant_id, { input: {} }))
      .toMatchObject({ text: "first" });
    const second = await client.assistants.update(created.assistant_id, {
      config: { configurable: { text: "second" } },
    });
    expect(second.version).toBe(2);
    expect(second.context).toEqual({ tenant: "example" });
    const secondThread = await client.threads.create();
    expect(await client.runs.wait(secondThread.thread_id, created.assistant_id, { input: {} }))
      .toMatchObject({ text: "second" });
    const overrideThread = await client.threads.create();
    expect(await client.runs.wait(overrideThread.thread_id, created.assistant_id, {
      input: {}, config: { configurable: { text: "override" } },
    })).toMatchObject({ text: "override" });
    expect((await client.assistants.search()).find(item => item.assistant_id === created.assistant_id)?.version).toBe(2);
    expect((await client.assistants.getVersions(created.assistant_id)).map(item => item.version)).toEqual([2, 1]);
    const rolled = await client.assistants.setLatest(created.assistant_id, 1);
    expect(rolled.config.configurable).toEqual({ text: "first" });
    expect((await client.assistants.get(created.assistant_id)).version).toBe(1);
    const rolledThread = await client.threads.create();
    expect(await client.runs.wait(rolledThread.thread_id, created.assistant_id, { input: {} }))
      .toMatchObject({ text: "first" });
    await client.assistants.delete(created.assistant_id);
    expect(await versions.currentVersion(created.assistant_id)).toBeNull();
  } finally {
    await runtime.close();
  }
});

test("authorization filters thread reads and replaces create payloads", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite", url: ":memory:" } });
  try {
    const alice = await runtime.store.createThread({ metadata: { team: "alice" } });
    const bob = await runtime.store.createThread({ metadata: { team: "bob" } });
    const provider: AuthProvider = {
      authenticate(request) { return { identity: request.headers.get("x-user") ?? "" }; },
      authorize(context, value) {
        if (context.resource !== "threads") return true;
        if (context.action === "create") return { ...value, metadata: { team: context.user.identity } };
        if (["search", "read", "delete"].includes(context.action)) {
          return { metadata: { team: context.user.identity } };
        }
        return true;
      },
    };
    const app = new Hono();
    app.use("*", authMiddleware(provider));
    app.route("/", createApi(createPlatformAdapter(runtime, runtime.store, [])));
    const request = (user: string, path: string, body?: Record<string, unknown>) =>
      app.request(path, { method: body ? "POST" : "GET",
        headers: { "x-user": user, "content-type": "application/json" },
        body: body ? JSON.stringify(body) : undefined });

    const created = await request("alice", "/threads", { metadata: { team: "bob" } });
    expect(created.status).toBe(200);
    const createdBody = await created.json() as { metadata: { team: string } };
    expect(createdBody.metadata.team).toBe("alice");
    const search = await request("alice", "/threads/search", {});
    const items = await search.json() as Array<{ thread_id: string }>;
    expect(items.some(item => item.thread_id === alice.id)).toBe(true);
    expect(items.some(item => item.thread_id === bob.id)).toBe(false);
    expect((await request("alice", `/threads/${bob.id}`)).status).toBe(403);
  } finally {
    await runtime.close();
  }
});

test("authenticated stateless runs keep their ephemeral thread private", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite", url: ":memory:" } });
  try {
    runtime.registerGraph({ id: "echo", entrypoint: "reply", nodes: { reply: value => value } });
    await seedDefaultAssistants(runtime.store, runtime.listGraphs());
    const app = new Hono();
    app.use("*", authMiddleware({
      authenticate(request) { return { identity: request.headers.get("x-user") ?? "" }; },
    }));
    app.route("/", createApi(createPlatformAdapter(runtime, runtime.store, runtime.listGraphs())));
    const created = await app.request("/runs", { method: "POST",
      headers: { "x-user": "alice", "content-type": "application/json" },
      body: JSON.stringify({ assistant_id: "echo", input: { message: "secret" } }),
    });
    expect(created.status).toBe(200);
    const run = await created.json() as { run_id: string; thread_id: string };
    expect((await runtime.store.getThread(run.thread_id))?.metadata._owner).toBe("alice");
    expect((await app.request(`/runs/${run.run_id}`, { headers: { "x-user": "bob" } })).status).toBe(403);
    expect((await app.request(`/runs/${run.run_id}`, { headers: { "x-user": "alice" } })).status).toBe(200);
  } finally {
    await runtime.close();
  }
});

test("cancelling a queued run releases the thread and terminates its stream", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite", url: ":memory:" }, inline: false });
  try {
    runtime.registerGraph({ id: "counter", entrypoint: "add", nodes: {
      add: value => ({ count: Number(value.count ?? 0) + 1 }),
    } });
    await seedDefaultAssistants(runtime.store, runtime.listGraphs());
    const adapter = createPlatformAdapter(runtime, runtime.store, runtime.listGraphs());
    const context = { request: new Request("http://valida.test") };
    const thread = await adapter.threads.create({}, context);
    const run = await adapter.runs.create(thread.thread_id, {
      assistant_id: "counter", input: { count: 1 },
    }, context);
    expect(run.status).toBe("pending");
    expect((await adapter.threads.get(thread.thread_id, context))?.status).toBe("busy");
    expect(await adapter.runs.cancel(thread.thread_id, run.run_id, "interrupt", context)).toBe(true);
    expect((await adapter.threads.get(thread.thread_id, context))?.status).toBe("idle");
    const events = [];
    for await (const event of adapter.runs.events(thread.thread_id, run.run_id, null, context)) {
      events.push(event);
    }
    expect(events.at(-1)).toMatchObject({ event: "end", data: { status: "cancelled" } });
    const next = await adapter.runs.create(thread.thread_id, {
      assistant_id: "counter", input: { count: 2 },
    }, context);
    expect(next.status).toBe("pending");
  } finally {
    await runtime.close();
  }
});
