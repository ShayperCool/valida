import { expect, test } from "bun:test";
import { Client } from "@langchain/langgraph-sdk";
import { AIMessageChunk, HumanMessage } from "@langchain/core/messages";
import { FakeStreamingChatModel } from "@langchain/core/utils/testing";
import { tool } from "@langchain/core/tools";
import { Annotation, END, MessagesAnnotation, START, StateGraph } from "@langchain/langgraph";
import { z } from "zod";
import { createRuntime } from "../engine/index";
import { createPlatformAdapter, seedDefaultAssistants } from "../platform";
import { createApi } from "./index";
import { NativeV2Bridge } from "./native_v2";
import type { ApiRequestContext, StreamEvent } from "./types";

async function collect<T>(stream: AsyncIterable<T>, count: number, predicate: (event: T) => boolean): Promise<T[]> {
  const events: T[] = [];
  for await (const event of stream) {
    if (predicate(event)) events.push(event);
    if (events.length === count) return events;
  }
  return events;
}

test("SDK v2 stream preserves native token messages and subgraph tool namespaces", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite" } });
  const model = new FakeStreamingChatModel({ chunks: [new AIMessageChunk("He"), new AIMessageChunk("llo")] });
  const tokenGraph = new StateGraph(MessagesAnnotation)
    .addNode("reply", async state => ({ messages: [await model.invoke(state.messages)] }))
    .addEdge(START, "reply").addEdge("reply", END).compile();
  const state = Annotation.Root({ n: Annotation<number>, result: Annotation<string> });
  const add = tool(async ({ a, b }) => String(a + b), {
    name: "add", description: "Add numbers", schema: z.object({ a: z.number(), b: z.number() }),
  });
  const child = new StateGraph(state)
    .addNode("calculate", async input => ({ n: input.n + 1, result: await add.invoke({ a: input.n, b: 1 }) }))
    .addEdge(START, "calculate").addEdge("calculate", END).compile();
  const toolGraph = new StateGraph(state)
    .addNode("child", child).addEdge(START, "child").addEdge("child", END).compile();
  runtime.registerGraph({ id: "tokens", graph: tokenGraph });
  runtime.registerGraph({ id: "nested", graph: toolGraph });
  await seedDefaultAssistants(runtime.store, runtime.listGraphs());
  const adapter = createPlatformAdapter(runtime, runtime.store, runtime.listGraphs());
  adapter.v2 = new NativeV2Bridge(adapter, runtime);
  const app = createApi(adapter);
  const client = new Client({
    apiUrl: "http://valida.test", apiKey: null,
    callerOptions: { maxRetries: 0,
      fetch: ((input: RequestInfo | URL, init?: RequestInit) => app.fetch(new Request(input, init))) as typeof fetch },
  });
  const fetchForStream = ((input: RequestInfo | URL, init?: RequestInit) => app.fetch(new Request(input, init))) as typeof fetch;
  try {
    const tokenThread = await client.threads.create();
    const tokens = client.threads.stream(tokenThread.thread_id, {
      assistantId: "tokens", maxReconnectAttempts: 0, streamIdleReconnect: 0, fetch: fetchForStream,
    });
    try {
      const subscription = await tokens.subscribe({ channels: ["messages"], depth: 2 });
      const received = collect(subscription, 2, event =>
        event.method === "messages" &&
        (event.params.data as { event?: string }).event === "content-block-delta");
      await tokens.run.start({ input: { messages: [new HumanMessage("hi")] } });
      const deltas = await Promise.race([
        received,
        new Promise<never>((_resolve, reject) => setTimeout(() => reject(new Error("Token stream timed out")), 5000)),
      ]);
      expect(deltas.map(event => (event.params.data as { delta: { text: string } }).delta.text))
        .toEqual(["He", "llo"]);
      expect(deltas.every(event => event.params.namespace.length > 0 &&
        (event.params as { node?: string }).node === "reply")).toBe(true);
      expect((await tokens.values as { messages: unknown[] }).messages).toHaveLength(2);
      await subscription.unsubscribe();
    } finally {
      await tokens.close();
    }

    const toolThread = await client.threads.create();
    const tools = client.threads.stream(toolThread.thread_id, {
      assistantId: "nested", maxReconnectAttempts: 0, streamIdleReconnect: 0, fetch: fetchForStream,
    });
    try {
      const subscription = await tools.subscribe({ channels: ["tools"], namespaces: [["child"]], depth: 2 });
      const received = collect(subscription, 2, event => event.method === "tools");
      await tools.run.start({ input: { n: 2 } });
      const toolEvents = await Promise.race([
        received,
        new Promise<never>((_resolve, reject) => setTimeout(() => reject(new Error("Tool stream timed out")), 5000)),
      ]);
      expect(toolEvents.map(event => (event.params.data as { event: string }).event))
        .toEqual(["tool-started", "tool-finished"]);
      expect(toolEvents.every(event => event.params.namespace[0]?.startsWith("child:"))).toBe(true);
      expect(await tools.values).toMatchObject({ n: 3, result: "3" });
      await subscription.unsubscribe();
    } finally {
      await tools.close();
    }
  } finally {
    await runtime.close();
  }
});

test("mixed compiled and custom runs replay with one thread-wide sequence", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite" } });
  try {
    const state = Annotation.Root({ n: Annotation<number> });
    runtime.registerGraph({ id: "compiled", graph: new StateGraph(state)
      .addNode("double", input => ({ n: input.n * 2 }))
      .addEdge(START, "double").addEdge("double", END).compile() });
    runtime.registerGraph({ id: "custom", entrypoint: "triple", nodes: {
      triple: input => ({ n: Number(input.n) * 3 }),
    }, edges: { triple: END } });
    await seedDefaultAssistants(runtime.store, runtime.listGraphs());
    const adapter = createPlatformAdapter(runtime, runtime.store, runtime.listGraphs());
    const bridge = new NativeV2Bridge(adapter, runtime);
    const thread = await runtime.createThread();
    const context: ApiRequestContext = { request: new Request("http://valida.test") };
    const compiled = await adapter.runs.create(thread.id, { assistant_id: "compiled", input: { n: 2 } }, context);
    expect((await runtime.waitRun(compiled.run_id)).status).toBe("success");
    await new Promise(resolve => setTimeout(resolve, 5));
    const custom = await adapter.runs.create(thread.id, { assistant_id: "custom", input: { n: 3 } }, context);
    expect((await runtime.waitRun(custom.run_id)).status).toBe("success");

    const filter = { channels: ["lifecycle", "values", "updates", "checkpoints", "tasks", "input"] };
    const all: StreamEvent[] = [];
    let completed = 0;
    for await (const event of bridge.events(thread.id, filter, context)) {
      all.push(event);
      const envelope = event.data as { method: string; params: { data: { event?: string }; namespace: string[] } };
      if (envelope.method === "lifecycle" && envelope.params.data.event === "completed" &&
        envelope.params.namespace.length === 0) completed += 1;
      if (completed === 2) break;
    }
    expect(all.some(event => event.id?.startsWith("1"))).toBe(true);
    expect(all.some(event => (event.data as { event_id: string }).event_id.startsWith(`${compiled.run_id}:`))).toBe(true);
    expect(all.some(event => (event.data as { event_id: string }).event_id.startsWith(`${custom.run_id}:`))).toBe(true);
    const sequence = all.map(event => Number(event.id));
    expect(sequence.every((value, index) => index === 0 || value > sequence[index - 1]!)).toBe(true);
    expect(all.some(event => event.event === "values" &&
      (event.data as { params: { data: { n?: number } } }).params.data.n === 4)).toBe(true);
    expect(all.some(event => event.event === "values" &&
      (event.data as { params: { data: { n?: number } } }).params.data.n === 9)).toBe(true);

    const cursor = Number(all[2]!.id);
    const replay = bridge.events(thread.id, { ...filter, since: cursor }, context)[Symbol.asyncIterator]();
    try {
      const next = await replay.next();
      expect(next.value?.id).toBe(all[3]?.id);
      expect((next.value?.data as { event_id: string }).event_id)
        .toBe((all[3]?.data as { event_id: string }).event_id);
    } finally {
      await replay.return?.();
    }
  } finally {
    await runtime.close();
  }
});
