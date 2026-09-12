import { describe, expect, test } from "bun:test";
import { Client } from "@langchain/langgraph-sdk";
import { createApi, type PlatformAdapter } from "./index";

const now = "2026-01-01T00:00:00.000Z";

function fixture() {
  const received: { runPayload?: Record<string, unknown>; lastEventId?: string | null } = {};
  const assistant = {
    assistant_id: "agent",
    graph_id: "agent",
    name: "Deterministic agent",
    config: {},
    metadata: {},
    created_at: now,
    updated_at: now,
  };
  const thread = {
    thread_id: "thread-1",
    status: "idle" as const,
    metadata: {},
    created_at: now,
    updated_at: now,
  };
  const state = {
    values: { messages: [{ type: "ai", content: "done" }] },
    next: [],
    tasks: [],
    interrupts: [],
    metadata: {},
    checkpoint: { thread_id: "thread-1", checkpoint_ns: "", checkpoint_id: "checkpoint-1" },
    parent_checkpoint: null,
    created_at: now,
  };
  const run = {
    run_id: "run-1",
    thread_id: "thread-1",
    assistant_id: "agent",
    status: "success" as const,
    created_at: now,
    updated_at: now,
  };
  const adapter: PlatformAdapter = {
    assistants: {
      create: async () => assistant,
      search: async () => [assistant],
      get: async (id) => (id === "agent" ? assistant : null),
      update: async () => assistant,
      delete: async () => true,
      graph: async () => ({ nodes: [], edges: [] }),
      schemas: async () => ({ input_schema: {}, output_schema: {}, state_schema: {}, config_schema: {} }),
    },
    threads: {
      create: async () => thread,
      search: async () => [thread],
      get: async (id) => (id === "thread-1" ? thread : null),
      update: async () => thread,
      delete: async () => true,
      getState: async (_id, checkpoint) => (checkpoint === "unknown" ? null : state),
      updateState: async () => ({ checkpoint: state.checkpoint }),
      history: async () => [state],
    },
    runs: {
      create: async (_threadId, payload) => {
        received.runPayload = payload;
        return run;
      },
      get: async () => run,
      list: async () => [run],
      join: async () => state.values,
      events: async function* (_threadId, _runId, lastEventId) {
        received.lastEventId = lastEventId;
        yield { id: "1", event: "metadata", data: { run_id: "run-1" } };
        yield { id: "2", event: "values", data: state.values };
        yield { id: "3", event: "end", data: { status: "success" } };
      },
      cancel: async () => true,
    },
  };
  const app = createApi(adapter);
  const client = new Client({
    apiUrl: "http://valida.test",
    apiKey: null,
    callerOptions: {
      maxRetries: 0,
      fetch: ((input: RequestInfo | URL, init?: RequestInit) => app.fetch(new Request(input, init))) as typeof fetch,
    },
  });
  return { app, client, received };
}

describe("Agent Protocol HTTP compatibility", () => {
  test("LangGraph SDK reads assistants, checkpoints, and history", async () => {
    const { client } = fixture();
    expect((await client.assistants.get("agent")).graph_id).toBe("agent");
    expect((await client.assistants.search())[0]?.assistant_id).toBe("agent");
    const created = await client.threads.create();
    expect(created.thread_id).toBe("thread-1");
    expect((await client.threads.getState("thread-1")).checkpoint?.checkpoint_id).toBe("checkpoint-1");
    expect((await client.threads.getHistory("thread-1"))[0]?.values).toEqual({
      messages: [{ type: "ai", content: "done" }],
    });
  });

  test("SDK run wait accepts HITL command resume and returns values", async () => {
    const { client, received } = fixture();
    const values = await client.runs.wait("thread-1", "agent", { command: { resume: "approved" } });
    expect(values).toEqual({ messages: [{ type: "ai", content: "done" }] });
    expect(received.runPayload?.command).toEqual({ resume: "approved" });
  });

  test("SDK parses streaming SSE and reconnect uses Last-Event-ID", async () => {
    const { client, received } = fixture();
    const events = [];
    for await (const event of client.runs.stream("thread-1", "agent", { input: { value: 1 }, streamMode: "values" })) {
      events.push(event.event);
    }
    expect(events).toContain("values");
    const joined = [];
    for await (const event of client.runs.joinStream("thread-1", "run-1", { lastEventId: "1" })) {
      joined.push(event.event);
    }
    expect(joined).toContain("end");
    expect(received.lastEventId).toBe("1");
  });

  test("invalid run input is rejected before enqueue", async () => {
    const { app, received } = fixture();
    const response = await app.request("/threads/thread-1/runs", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ assistant_id: "agent", input: { x: 1 }, command: { resume: true } }),
    });
    expect(response.status).toBe(422);
    expect(received.runPayload).toBeUndefined();
  });

  test("SDK thread-scoped v2 stream receives values through the legacy bridge", async () => {
    const { app, client, received } = fixture();
    const thread = client.threads.stream("thread-1", {
      assistantId: "agent",
      maxReconnectAttempts: 0,
      streamIdleReconnect: 0,
      fetch: ((input: RequestInfo | URL, init?: RequestInit) => app.fetch(new Request(input, init))) as typeof fetch,
    });
    try {
      await thread.run.start({ input: { value: 1 } });
      expect(await thread.values).toMatchObject({ messages: [{ type: "ai", content: "done" }] });
      expect(received.runPayload?.input).toEqual({ value: 1 });
      await thread.input.respond({ interrupt_id: "interrupt-1", namespace: [], response: "approved" });
      expect(received.runPayload?.command).toEqual({ resume: "approved" });
    } finally {
      await thread.close();
    }
  });
});
