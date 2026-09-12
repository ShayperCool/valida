import { expect, test } from "bun:test";
import { Client } from "@langchain/langgraph-sdk";
import type { Assistant } from "./types.ts";
import { createApi } from "./index.ts";
import { Store, type AssistantRecord } from "../db/index.ts";
import { createAssistantVersionsExtension } from "../extensions/assistant_versions.ts";

const apiAssistant = (row: AssistantRecord): Assistant => ({
  assistant_id: row.id,
  graph_id: row.graphId,
  name: row.name,
  description: row.description,
  config: row.config,
  context: {},
  metadata: row.metadata,
  created_at: row.createdAt,
  updated_at: row.updatedAt,
});

test("SDK lists versions and promotes an older assistant config", async () => {
  const store = new Store({ dialect: "sqlite", url: ":memory:" });
  try {
    await store.migrate();
    const versions = await createAssistantVersionsExtension(store);
    const initial = apiAssistant(await store.createAssistant({
      id: "agent", graphId: "graph-a", name: "Agent", config: { configurable: { answer: "v1" } },
    }));
    await versions.recordCreated(initial);
    const second = apiAssistant((await store.updateAssistant("agent", { config: { configurable: { answer: "v2" } } }))!);
    expect((await versions.recordUpdated(initial, second)).version).toBe(2);

    const app = createApi({ assistants: {
      versions: versions.versions,
      setLatest: versions.setLatest,
    } } as Parameters<typeof createApi>[0]);
    const client = new Client({
      apiUrl: "http://valida.test",
      apiKey: null,
      callerOptions: {
        maxRetries: 0,
        fetch: (input: RequestInfo | URL, init?: RequestInit) => app.fetch(new Request(input, init)),
      },
    });

    const history = await client.assistants.getVersions("agent");
    expect(history.map((item) => item.version)).toEqual([2, 1]);
    expect(history.map((item) => item.config.configurable)).toEqual([{ answer: "v2" }, { answer: "v1" }]);
    expect((await client.assistants.getVersions("agent", { limit: 1 }))).toHaveLength(1);
    expect((await (await app.request("/assistants/agent/versions", { method: "POST" })).json() as Assistant[]).length).toBe(2);

    const rolled = await client.assistants.setLatest("agent", 1);
    expect(rolled.version).toBe(1);
    expect((await store.getAssistant("agent"))?.config).toEqual({ configurable: { answer: "v1" } });
    expect(await versions.currentVersion("agent")).toBe(1);
    const queryPromote = await app.request("/assistants/agent/latest?version=2", { method: "POST" });
    expect(queryPromote.status).toBe(200);
    expect((await queryPromote.json() as Assistant).version).toBe(2);
    await client.assistants.setLatest("agent", 1);

    const third = apiAssistant((await store.updateAssistant("agent", { config: { configurable: { answer: "v3" } } }))!);
    expect((await versions.recordUpdated(rolled as Assistant, third)).version).toBe(3);
    expect((await client.assistants.getVersions("agent")).map((item) => item.version)).toEqual([3, 2, 1]);
    const reopened = await createAssistantVersionsExtension(store);
    expect((await reopened.versions("agent", {})).map((item) => item.version)).toEqual([3, 2, 1]);
    await store.createAssistant({ id: "default", graphId: "graph-a", name: "Default" });
    expect(await reopened.currentVersion("default")).toBe(1);

    const missing = await app.request("/assistants/agent/latest", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ version: 99 }),
    });
    expect(missing.status).toBe(404);
    const unconfigured = createApi({ assistants: {} } as Parameters<typeof createApi>[0]);
    expect((await unconfigured.request("/assistants/agent/versions", { method: "POST" })).status).toBe(501);
    expect((await unconfigured.request("/assistants/agent/latest", { method: "POST" })).status).toBe(501);
  } finally {
    await store.close();
  }
});
