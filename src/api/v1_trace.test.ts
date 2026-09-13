import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Client } from "@langchain/langgraph-sdk";
import { AIMessageChunk, HumanMessage } from "@langchain/core/messages";
import { FakeStreamingChatModel } from "@langchain/core/utils/testing";
import { END, MessagesAnnotation, START, StateGraph } from "@langchain/langgraph";
import { createRuntime, type GraphRuntime } from "../engine/index";
import { createPlatformAdapter, seedDefaultAssistants } from "../platform";
import { createApi } from "./index";
import { V1StreamBridge } from "./v1_stream";

type Trace = { event: string; name: string; run_id: string; tags: string[];
  metadata: Record<string, unknown>; parent_ids: string[]; data: Record<string, unknown> };
type SdkEvent = { id?: string; event: string; data: unknown };

test("captured v1 callbacks retain real tags and parent IDs across durable SDK replay", async () => {
  const dir = await mkdtemp(join(tmpdir(), "valida-v1-trace-"));
  const url = join(dir, "trace.sqlite");
  let executions = 0;
  const makeGraph = () => {
    const model = new FakeStreamingChatModel({ chunks: [new AIMessageChunk("A"), new AIMessageChunk("B")] })
      .withConfig({ tags: ["model-tag"] });
    return new StateGraph(MessagesAnnotation)
      .addNode("reply", async state => {
        executions++;
        return { messages: [await model.invoke(state.messages)] };
      })
      .addEdge(START, "reply").addEdge("reply", END).compile();
  };
  let runtime: GraphRuntime | null = null;
  try {
    runtime = await createRuntime({ db: { dialect: "sqlite", url } });
    runtime.registerGraph({ id: "trace", graph: makeGraph() });
    await seedDefaultAssistants(runtime.store, runtime.listGraphs());
    let adapter = createPlatformAdapter(runtime, runtime.store, runtime.listGraphs());
    adapter.v1 = new V1StreamBridge(adapter, runtime);
    let app = createApi(adapter);
    const client = () => new Client({ apiUrl: "http://valida.test", apiKey: null,
      callerOptions: { maxRetries: 0,
        fetch: ((input: RequestInfo | URL, init?: RequestInit) => app.fetch(new Request(input, init))) as typeof fetch } });
    const thread = await client().threads.create();
    const first: SdkEvent[] = [];
    for await (const event of client().runs.stream(thread.thread_id, "trace", {
      input: { messages: [new HumanMessage("hi")] },
      config: { tags: ["root-tag"] }, streamMode: ["events"],
    })) first.push(event);
    const run = (await client().runs.list(thread.thread_id))[0]!;
    expect((await runtime.waitRun(run.run_id)).status).toBe("success");
    const traces = first.filter(item => item.event === "events").map(item => item.data as Trace);
    const root = traces.find(item => item.event === "on_chain_start" && item.parent_ids.length === 0);
    const node = traces.find(item => item.event === "on_chain_start" && item.name === "reply");
    const model = traces.find(item => item.event === "on_chat_model_start");
    expect(root?.tags).toContain("root-tag");
    expect(node?.parent_ids).toEqual([root!.run_id]);
    expect(model?.parent_ids).toEqual([root!.run_id, node!.run_id]);
    expect(model?.tags).toEqual(expect.arrayContaining(["root-tag", "model-tag"]));
    expect(model?.metadata.langgraph_node).toBe("reply");
    expect(traces.filter(item => item.event === "on_chat_model_stream")
      .map(item => (item.data.chunk as { content: string }).content)).toEqual(["A", "B"]);
    expect((await runtime.store.listEvents(run.run_id)).some(item => item.event === "trace")).toBe(true);
    const legacy = [];
    for await (const event of runtime.stream(run.run_id)) legacy.push(event.event);
    expect(legacy).not.toContain("trace");
    await runtime.close();
    runtime = await createRuntime({ db: { dialect: "sqlite", url } });
    runtime.registerGraph({ id: "trace", graph: makeGraph() });
    adapter = createPlatformAdapter(runtime, runtime.store, runtime.listGraphs());
    adapter.v1 = new V1StreamBridge(adapter, runtime);
    app = createApi(adapter);
    const replay: SdkEvent[] = [];
    for await (const event of client().runs.joinStream(thread.thread_id, run.run_id, {
      streamMode: ["events"],
    })) replay.push(event);
    expect(replay.filter(item => item.event === "events")).toEqual(first.filter(item => item.event === "events"));
    expect(executions).toBe(1);
  } finally {
    await runtime?.close();
    await rm(dir, { recursive: true, force: true });
  }
});
