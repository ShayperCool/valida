import { expect, test } from "bun:test";
import { HttpAgent } from "@ag-ui/client";
import { EventType, RunAgentInputSchema, type BaseEvent } from "@ag-ui/core";
import { AIMessage, ToolMessage } from "@langchain/core/messages";
import { Annotation, END, interrupt, MessagesAnnotation, START, StateGraph } from "@langchain/langgraph";
import { Hono } from "hono";
import { authMiddleware } from "../auth.ts";
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
    const events: BaseEvent[] = [];
    agent.subscribe({ onEvent: ({ event }) => { events.push(event); } });
    agent.addMessage({ id: "proposal", role: "user", content: "Ship" });
    await agent.runAgent();
    expect(events.map(event => event.type)).toContain(EventType.MESSAGES_SNAPSHOT);
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

test("official HttpAgent can return a client-executed tool result on the next turn", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite" } });
  try {
    runtime.registerGraph({ id: "client_tool", graph: new StateGraph(MessagesAnnotation)
      .addNode("reply", state => {
        const last = state.messages.at(-1);
        return { messages: [last?.type === "tool"
          ? new AIMessage(`Result: ${last.content}`)
          : new AIMessage({ content: "", tool_calls: [{ id: "client-call", name: "lookup",
            args: { key: "fixed" } }] })] };
      })
      .addEdge(START, "reply").addEdge("reply", END).compile() });
    await seedDefaultAssistants(runtime.store, runtime.listGraphs());
    const adapter = createPlatformAdapter(runtime, runtime.store, runtime.listGraphs());
    const app = createAgUiApi(adapter, runtime);
    const agent = new HttpAgent({ url: "http://valida.test/ag-ui/client_tool",
      fetch: async (url, init) => app.fetch(new Request(url, init)) });
    agent.addMessage({ id: "lookup-request", role: "user", content: "Search" });
    await agent.runAgent({ tools: [{ name: "lookup", description: "Look up a key",
      parameters: { type: "object", properties: { key: { type: "string" } } } }] });
    expect(agent.messages.some(message => message.role === "assistant" &&
      message.toolCalls?.[0]?.id === "client-call")).toBe(true);
    agent.addMessage({ id: "client-result", role: "tool", toolCallId: "client-call", content: "found" });
    await agent.runAgent();
    expect(agent.messages.at(-1)?.content).toBe("Result: found");
    const state = await adapter.threads.getState(agent.threadId, null,
      { request: new Request("http://valida.test") });
    expect((state?.values.messages as Array<{ id: string }>).filter(message => message.id === "client-result"))
      .toHaveLength(1);
  } finally {
    await runtime.close();
  }
});

test("AG-UI cancelled resume passes the LangGraph cancellation sentinel", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite" } });
  try {
    const schema = Annotation.Root({ cancelled: Annotation<boolean> });
    runtime.registerGraph({ id: "cancel", graph: new StateGraph(schema)
      .addNode("ask", () => {
        const answer = interrupt("Continue?") as { __agui_cancelled__?: boolean };
        return { cancelled: answer.__agui_cancelled__ === true };
      })
      .addEdge(START, "ask").addEdge("ask", END).compile() });
    await seedDefaultAssistants(runtime.store, runtime.listGraphs());
    const adapter = createPlatformAdapter(runtime, runtime.store, runtime.listGraphs());
    const app = createAgUiApi(adapter, runtime);
    const agent = new HttpAgent({ url: "http://valida.test/ag-ui/cancel",
      fetch: async (url, init) => app.fetch(new Request(url, init)) });
    await agent.runAgent();
    expect(agent.pendingInterrupts).toHaveLength(1);
    const interruptId = agent.pendingInterrupts[0]!.id;
    await agent.runAgent({ resume: [{ interruptId, status: "cancelled" }] });
    expect(agent.pendingInterrupts).toHaveLength(0);
    expect((await adapter.threads.getState(agent.threadId, null,
      { request: new Request("http://valida.test") }))?.values.cancelled).toBe(true);
  } finally {
    await runtime.close();
  }
});

test("AG-UI resumes all parallel interrupts with one ID-keyed map", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite" } });
  try {
    const schema = Annotation.Root({ first: Annotation<unknown>, second: Annotation<unknown> });
    runtime.registerGraph({ id: "parallel", graph: new StateGraph(schema)
      .addNode("ask_first", () => ({ first: interrupt("First?") }))
      .addNode("ask_second", () => ({ second: interrupt("Second?") }))
      .addEdge(START, "ask_first").addEdge(START, "ask_second")
      .addEdge("ask_first", END).addEdge("ask_second", END).compile() });
    await seedDefaultAssistants(runtime.store, runtime.listGraphs());
    const adapter = createPlatformAdapter(runtime, runtime.store, runtime.listGraphs());
    const app = createAgUiApi(adapter, runtime);
    const agent = new HttpAgent({ url: "http://valida.test/ag-ui/parallel",
      fetch: async (url, init) => app.fetch(new Request(url, init)) });
    await agent.runAgent();
    expect(agent.pendingInterrupts).toHaveLength(2);
    const pending = agent.pendingInterrupts;
    await agent.runAgent({ resume: [
      { interruptId: pending[0]!.id, status: "resolved", payload: "yes" },
      { interruptId: pending[1]!.id, status: "cancelled" },
    ] });
    expect(agent.pendingInterrupts).toHaveLength(0);
    const state = await adapter.threads.getState(agent.threadId, null,
      { request: new Request("http://valida.test") });
    const expected = { __agui_resume_map__: {
      [pending[0]!.id]: { status: "resolved", payload: "yes" },
      [pending[1]!.id]: { status: "cancelled" },
    } };
    expect(state?.values.first).toEqual(expected);
    expect(state?.values.second).toEqual(expected);
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

test("official HttpAgent forwards client tools, context, and multimodal user content", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite" } });
  try {
    const observed: { tools?: unknown; context?: unknown; content?: unknown } = {};
    runtime.registerGraph({ id: "inspect_input", graph: new StateGraph(MessagesAnnotation)
      .addNode("inspect", (state, config) => {
        observed.tools = config.configurable?.ag_ui_tools;
        observed.context = config.configurable?.ag_ui_context;
        observed.content = state.messages.at(-1)?.content;
        return { messages: [new AIMessage("received")] };
      })
      .addEdge(START, "inspect").addEdge("inspect", END).compile() });
    await seedDefaultAssistants(runtime.store, runtime.listGraphs());
    const app = createAgUiApi(createPlatformAdapter(runtime, runtime.store, runtime.listGraphs()), runtime);
    const agent = new HttpAgent({ url: "http://valida.test/ag-ui/inspect_input",
      fetch: async (url, init) => app.fetch(new Request(url, init)) });
    const content = [{ type: "text" as const, text: "Describe" },
      { type: "image" as const, source: { type: "url" as const,
        value: "https://example.test/image.png", mimeType: "image/png" } }];
    const tools = [{ name: "lookup", description: "Look up a fixed key", parameters: {
      type: "object", properties: { key: { type: "string" } } } }];
    const context = [{ description: "locale", value: "en" }];
    agent.addMessage({ id: "multimodal-user", role: "user", content });
    await agent.runAgent({ tools, context });
    expect(agent.messages.at(-1)?.content).toBe("received");
    expect(observed).toEqual({ tools, context, content });
  } finally {
    await runtime.close();
  }
});

test("AG-UI streams state deltas between deterministic graph checkpoints", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite" } });
  try {
    const schema = Annotation.Root({ count: Annotation<number> });
    runtime.registerGraph({ id: "state_delta", graph: new StateGraph(schema)
      .addNode("increment_once", () => ({ count: 1 }))
      .addNode("increment_twice", () => ({ count: 2 }))
      .addEdge(START, "increment_once").addEdge("increment_once", "increment_twice")
      .addEdge("increment_twice", END).compile() });
    await seedDefaultAssistants(runtime.store, runtime.listGraphs());
    const app = createAgUiApi(createPlatformAdapter(runtime, runtime.store, runtime.listGraphs()), runtime);
    const agent = new HttpAgent({ url: "http://valida.test/ag-ui/state_delta",
      fetch: async (url, init) => app.fetch(new Request(url, init)) });
    const events: BaseEvent[] = [];
    agent.subscribe({ onEvent: ({ event }) => { events.push(event); } });
    await agent.runAgent();
    expect(events.some(event => event.type === EventType.STATE_SNAPSHOT)).toBe(true);
    expect(events.some(event => event.type === EventType.STATE_DELTA)).toBe(true);
    expect(agent.state).toEqual({ count: 2 });
  } finally {
    await runtime.close();
  }
});

test("AG-UI projects deterministic reasoning blocks without an LLM", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite" } });
  try {
    runtime.registerGraph({ id: "reasoning", graph: new StateGraph(MessagesAnnotation)
      .addNode("reply", () => ({ messages: [new AIMessage({ content: [
        { type: "reasoning", reasoning: "Check the fixed input" },
        { type: "text", text: "Done" },
      ] })] }))
      .addEdge(START, "reply").addEdge("reply", END).compile() });
    await seedDefaultAssistants(runtime.store, runtime.listGraphs());
    const app = createAgUiApi(createPlatformAdapter(runtime, runtime.store, runtime.listGraphs()), runtime);
    const agent = new HttpAgent({ url: "http://valida.test/ag-ui/reasoning",
      fetch: async (url, init) => app.fetch(new Request(url, init)) });
    const events: BaseEvent[] = [];
    agent.subscribe({ onEvent: ({ event }) => { events.push(event); } });
    await agent.runAgent();
    expect(events.map(event => event.type)).toContain(EventType.REASONING_START);
    expect(events.map(event => event.type)).toContain(EventType.REASONING_MESSAGE_CONTENT);
    expect(events.map(event => event.type)).toContain(EventType.REASONING_END);
    expect(agent.messages.some(message => message.role === "assistant" && message.content === "Done")).toBe(true);
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

test("AG-UI uses the configured authentication and run authorization", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite" } });
  try {
    runtime.registerGraph({ id: "echo", graph: echo });
    await seedDefaultAssistants(runtime.store, runtime.listGraphs());
    const app = new Hono();
    app.use("*", authMiddleware({
      authenticate(request) {
        if (request.headers.get("authorization") !== "Bearer valid") throw new Error("Invalid token");
        return { identity: "alice" };
      },
      authorize(context) { return context.resource === "threads" && context.action === "create_run"; },
    }));
    app.route("/", createAgUiApi(createPlatformAdapter(runtime, runtime.store, runtime.listGraphs()), runtime));
    const body = JSON.stringify({ threadId: crypto.randomUUID() });
    const request = (authorization?: string) => app.request("/ag-ui/echo", {
      method: "POST", headers: { "content-type": "application/json", ...(authorization ? { authorization } : {}) }, body,
    });
    expect((await request()).status).toBe(401);
    expect((await request("Bearer valid")).status).toBe(422);
  } finally {
    await runtime.close();
  }
});
