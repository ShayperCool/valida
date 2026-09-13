import { expect, test } from "bun:test";
import { HttpAgent } from "@ag-ui/client";
import { EventType, RunAgentInputSchema, type BaseEvent } from "@ag-ui/core";
import { AIMessage, ToolMessage } from "@langchain/core/messages";
import { END, MessagesAnnotation, START, StateGraph } from "@langchain/langgraph";
import { createRuntime } from "../engine/index.ts";
import { createPlatformAdapter, seedDefaultAssistants } from "../platform.ts";
import { approval, echo } from "../../examples/graphs.ts";
import { createAgUiApi } from "./ag_ui.ts";

test("official AG-UI HttpAgent streams a deterministic graph and reuses its checkpoint", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite" } });
  try {
    runtime.registerGraph({ id: "echo", graph: echo });
    await seedDefaultAssistants(runtime.store, runtime.listGraphs());
    const adapter = createPlatformAdapter(runtime, runtime.store, runtime.listGraphs());
    const app = createAgUiApi(adapter, runtime);
    const agent = new HttpAgent({ url: "http://valida.test/ag-ui/echo",
      fetch: async (url, init) => app.fetch(new Request(url, init)) });
    const events: BaseEvent[] = [];
    agent.subscribe({ onEvent: ({ event }) => { events.push(event); } });
    agent.addMessage({ id: "user-1", role: "user", content: "first" });
    await agent.runAgent();
    expect(events.map(event => event.type)).toContain(EventType.RUN_STARTED);
    expect(events.map(event => event.type)).toContain(EventType.STATE_SNAPSHOT);
    expect(events.map(event => event.type)).toContain(EventType.TEXT_MESSAGE_CONTENT);
    expect(events.at(-1)?.type).toBe(EventType.RUN_FINISHED);
    expect(agent.messages.at(-1)?.content).toBe("Echo: first");
    expect((await adapter.threads.getState(agent.threadId, null, { request: new Request("http://valida.test") }))
      ?.checkpoint?.checkpoint_id).toBeTruthy();

    agent.addMessage({ id: "user-2", role: "user", content: "second" });
    await agent.runAgent();
    expect(agent.messages.at(-1)?.content).toBe("Echo: second");
    expect(agent.messages.filter(message => message.role === "assistant" && message.content === "Echo: first"))
      .toHaveLength(1);
    const state = await adapter.threads.getState(agent.threadId, null,
      { request: new Request("http://valida.test") });
    const messages = state?.values.messages as Array<{ id: string; content: string }>;
    expect(messages.map(message => message.id).filter(id => id === "user-1")).toHaveLength(1);
    expect(messages.map(message => message.id).filter(id => id === "user-2")).toHaveLength(1);
  } finally {
    await runtime.close();
  }
});

test("AG-UI returns canonical interrupt outcome and resumes the same thread", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite" } });
  try {
    runtime.registerGraph({ id: "approval", graph: approval });
    await seedDefaultAssistants(runtime.store, runtime.listGraphs());
    const adapter = createPlatformAdapter(runtime, runtime.store, runtime.listGraphs());
    const app = createAgUiApi(adapter, runtime);
    const agent = new HttpAgent({ url: "http://valida.test/ag-ui/approval",
      fetch: async (url, init) => app.fetch(new Request(url, init)) });
    agent.addMessage({ id: "proposal", role: "user", content: "Ship" });
    await agent.runAgent();
    expect(agent.pendingInterrupts).toHaveLength(1);
    const interrupt = agent.pendingInterrupts[0]!;
    expect(interrupt.reason).toBe("human_input");
    expect(interrupt.message).toContain("Ship");
    await agent.runAgent({ resume: [{ interruptId: interrupt.id, status: "resolved",
      payload: { decisions: [{ type: "approve" }] } }] });
    expect(agent.pendingInterrupts).toHaveLength(0);
    expect(agent.messages.at(-1)?.content).toBe("Approved: Ship");
    const state = await adapter.threads.getState(agent.threadId, null,
      { request: new Request("http://valida.test") });
    expect(state?.values).toMatchObject({ approved: true, result: "Approved: Ship" });
  } finally {
    await runtime.close();
  }
});

test("AG-UI projects deterministic graph tool calls and tool results", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite" } });
  try {
    const graph = new StateGraph(MessagesAnnotation)
      .addNode("call", () => ({ messages: [new AIMessage({ content: "",
        tool_calls: [{ id: "tool-1", name: "lookup", args: { key: "fixed" } }] })] }))
      .addNode("result", () => ({ messages: [new ToolMessage({ content: "found",
        tool_call_id: "tool-1" })] }))
      .addEdge(START, "call").addEdge("call", "result").addEdge("result", END).compile();
    runtime.registerGraph({ id: "tool_graph", graph });
    await seedDefaultAssistants(runtime.store, runtime.listGraphs());
    const adapter = createPlatformAdapter(runtime, runtime.store, runtime.listGraphs());
    const app = createAgUiApi(adapter, runtime);
    const agent = new HttpAgent({ url: "http://valida.test/ag-ui/tool_graph",
      fetch: async (url, init) => app.fetch(new Request(url, init)) });
    const events: BaseEvent[] = [];
    agent.subscribe({ onEvent: ({ event }) => { events.push(event); } });
    agent.addMessage({ id: "u", role: "user", content: "lookup" });
    await agent.runAgent();
    expect(events.map(event => event.type)).toContain(EventType.TOOL_CALL_START);
    expect(events.map(event => event.type)).toContain(EventType.TOOL_CALL_ARGS);
    expect(events.map(event => event.type)).toContain(EventType.TOOL_CALL_END);
    expect(events.map(event => event.type)).toContain(EventType.TOOL_CALL_RESULT);
    expect(agent.messages.some(message => message.role === "tool" && message.content === "found")).toBe(true);
  } finally {
    await runtime.close();
  }
});

test("AG-UI validates official input schema before creating a run", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite" } });
  try {
    runtime.registerGraph({ id: "echo", graph: echo });
    await seedDefaultAssistants(runtime.store, runtime.listGraphs());
    const adapter = createPlatformAdapter(runtime, runtime.store, runtime.listGraphs());
    const app = createAgUiApi(adapter, runtime);
    const invalid = await app.request("/ag-ui/echo", { method: "POST",
      headers: { "content-type": "application/json" }, body: JSON.stringify({ threadId: "x" }) });
    expect(invalid.status).toBe(422);
    const valid = RunAgentInputSchema.safeParse({ threadId: crypto.randomUUID(), runId: crypto.randomUUID(),
      state: {}, messages: [{ id: "u", role: "user", content: "hi" }], tools: [],
      context: [], forwardedProps: {} });
    expect(valid.success).toBe(true);
  } finally {
    await runtime.close();
  }
});
