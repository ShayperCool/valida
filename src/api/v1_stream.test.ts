import { expect, test } from "bun:test";
import { Client } from "@langchain/langgraph-sdk";
import { AIMessageChunk, HumanMessage } from "@langchain/core/messages";
import { FakeStreamingChatModel } from "@langchain/core/utils/testing";
import { Annotation, END, getWriter, MessagesAnnotation, START, StateGraph } from "@langchain/langgraph";
import { createRuntime } from "../engine/index";
import { createPlatformAdapter, seedDefaultAssistants } from "../platform";
import { createApi } from "./index";
import { V1StreamBridge } from "./v1_stream";

interface SdkEvent { id?: string; event: string; data: unknown }

test("SDK v1 run stream projects every mode from durable native events", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite" } });
  try {
    const model = new FakeStreamingChatModel({ chunks: [new AIMessageChunk("He"), new AIMessageChunk("llo")] });
    const graph = new StateGraph(MessagesAnnotation)
      .addNode("reply", async state => {
        getWriter()?.({ progress: "replying" });
        return { messages: [await model.invoke(state.messages)] };
      })
      .addEdge(START, "reply").addEdge("reply", END).compile();
    runtime.registerGraph({ id: "chat", graph });
    await seedDefaultAssistants(runtime.store, runtime.listGraphs());
    const adapter = createPlatformAdapter(runtime, runtime.store, runtime.listGraphs());
    adapter.v1 = new V1StreamBridge(adapter, runtime);
    const app = createApi(adapter);
    const client = new Client({ apiUrl: "http://valida.test", apiKey: null,
      callerOptions: { maxRetries: 0,
        fetch: ((input: RequestInfo | URL, init?: RequestInit) => app.fetch(new Request(input, init))) as typeof fetch } });
    const thread = await client.threads.create();
    const parts: SdkEvent[] = [];
    for await (const part of client.runs.stream(thread.thread_id, "chat", {
      input: { messages: [new HumanMessage("hi")] },
      streamMode: ["values", "updates", "messages", "custom", "events", "debug", "tasks", "checkpoints"],
    })) parts.push(part);
    expect(parts[0]?.event).toBe("metadata");
    const types = parts.map(part => part.event);
    for (const name of ["values", "updates", "messages/metadata", "messages/partial", "messages/complete",
      "custom", "events", "debug", "tasks", "checkpoints", "end"]) expect(types).toContain(name);
    const partial = parts.filter(part => part.event === "messages/partial")
      .map(part => ((part.data as Array<{ content: string }>)[0])?.content);
    expect(partial).toEqual(["He", "Hello"]);
    expect((parts.find(part => part.event === "messages/complete")?.data as Array<{ content: string }>)[0]?.content)
      .toBe("Hello");
    expect(parts.find(part => part.event === "custom")?.data).toEqual({ progress: "replying" });
    expect(parts.some(part => part.event === "events" &&
      (part.data as { event?: string }).event === "on_chat_model_stream")).toBe(true);
    expect(parts.some(part => part.event === "debug" &&
      (part.data as { type?: string }).type === "checkpoint")).toBe(true);

    const run = (await client.runs.list(thread.thread_id))[0]!;
    const cursor = parts.find(part => part.event === "messages/metadata")?.id;
    expect(cursor).toBeTruthy();
    const replay: SdkEvent[] = [];
    for await (const part of client.runs.joinStream(thread.thread_id, run.run_id, {
      lastEventId: cursor, streamMode: ["values", "updates", "messages", "custom", "events", "debug", "tasks", "checkpoints"],
    })) replay.push(part);
    expect(replay[0]?.id).toBe(parts[parts.findIndex(part => part.id === cursor) + 1]?.id);
    expect(replay.some(part => part.event === "messages/complete")).toBe(true);

    const tupleThread = await client.threads.create();
    const tuples: SdkEvent[] = [];
    for await (const part of client.runs.stream(tupleThread.thread_id, "chat", {
      input: { messages: [new HumanMessage("hi")] }, streamMode: ["messages-tuple", "values"],
    })) tuples.push(part);
    const chunks = tuples.filter(part => part.event === "messages")
      .map(part => part.data as unknown as [{ type: string; content: string }, { langgraph_node: string }]);
    expect(chunks.map(chunk => chunk[0].content)).toEqual(["He", "llo"]);
    expect(chunks.every(chunk => chunk[0].type === "AIMessageChunk" && chunk[1].langgraph_node === "reply"))
      .toBe(true);
    expect(tuples.some(part => part.event === "messages/partial")).toBe(false);
  } finally {
    await runtime.close();
  }
});

test("SDK v1 default values and explicit updates work for custom graphs", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite" } });
  try {
    runtime.registerGraph({ id: "double", entrypoint: "calculate", nodes: {
      calculate: state => ({ result: Number(state.input) * 2 }),
    }, edges: { calculate: END } });
    await seedDefaultAssistants(runtime.store, runtime.listGraphs());
    const adapter = createPlatformAdapter(runtime, runtime.store, runtime.listGraphs());
    adapter.v1 = new V1StreamBridge(adapter, runtime);
    const app = createApi(adapter);
    const client = new Client({ apiUrl: "http://valida.test", apiKey: null,
      callerOptions: { maxRetries: 0,
        fetch: ((input: RequestInfo | URL, init?: RequestInit) => app.fetch(new Request(input, init))) as typeof fetch } });
    const thread = await client.threads.create();
    const defaults: SdkEvent[] = [];
    for await (const part of client.runs.stream(thread.thread_id, "double", { input: { input: 3 } })) defaults.push(part);
    expect(defaults.map(part => part.event)).toEqual(["metadata", "values", "values", "end"]);
    const thread2 = await client.threads.create();
    const updates: SdkEvent[] = [];
    for await (const part of client.runs.stream(thread2.thread_id, "double", {
      input: { input: 4 }, streamMode: ["updates", "events", "debug"],
    })) updates.push(part);
    expect(updates.find(part => part.event === "updates")?.data)
      .toEqual({ calculate: { result: 8 } });
    expect(updates.map(part => part.event)).toContain("events");
    expect(updates.map(part => part.event)).toContain("debug");
  } finally {
    await runtime.close();
  }
});

test("SDK v1 stream_subgraphs preserves nested event names", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite" } });
  try {
    const state = Annotation.Root({ n: Annotation<number> });
    const child = new StateGraph(state).addNode("inc", input => ({ n: input.n + 1 }))
      .addEdge(START, "inc").addEdge("inc", END).compile();
    const graph = new StateGraph(state).addNode("child", child)
      .addEdge(START, "child").addEdge("child", END).compile();
    runtime.registerGraph({ id: "nested", graph });
    await seedDefaultAssistants(runtime.store, runtime.listGraphs());
    const adapter = createPlatformAdapter(runtime, runtime.store, runtime.listGraphs());
    adapter.v1 = new V1StreamBridge(adapter, runtime);
    const app = createApi(adapter);
    const client = new Client({ apiUrl: "http://valida.test", apiKey: null,
      callerOptions: { maxRetries: 0,
        fetch: ((input: RequestInfo | URL, init?: RequestInit) => app.fetch(new Request(input, init))) as typeof fetch } });
    const thread = await client.threads.create();
    const nested: SdkEvent[] = [];
    for await (const part of client.runs.stream(thread.thread_id, "nested", {
      input: { n: 1 }, streamMode: ["values", "updates"], streamSubgraphs: true,
    })) nested.push(part);
    expect(nested.some(part => part.event.startsWith("values|child:"))).toBe(true);
    expect(nested.some(part => part.event.startsWith("updates|child:"))).toBe(true);
    expect(nested.some(part => part.event === "values" && (part.data as { n?: number }).n === 2)).toBe(true);
    const rootThread = await client.threads.create();
    const rootOnly: SdkEvent[] = [];
    for await (const part of client.runs.stream(rootThread.thread_id, "nested", {
      input: { n: 1 }, streamMode: ["values", "updates"],
    })) rootOnly.push(part);
    expect(rootOnly.some(part => part.event.includes("|"))).toBe(false);
  } finally {
    await runtime.close();
  }
});
