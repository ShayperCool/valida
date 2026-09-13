import { expect, test } from "bun:test";
import { Annotation, END, START, StateGraph } from "@langchain/langgraph";
import { Client } from "@langchain/langgraph-sdk";
import { createRuntime } from "../engine/index.ts";
import { createPlatformAdapter, seedDefaultAssistants } from "../platform.ts";
import { createApi } from "./index.ts";

test("SDK updateState branches a compiled graph from the selected historical checkpoint", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite", url: ":memory:" } });
  try {
    const schema = Annotation.Root({ count: Annotation<number> });
    runtime.registerGraph({ id: "counter", graph: new StateGraph(schema)
      .addNode("increment", state => ({ count: (state.count ?? 0) + 1 }))
      .addEdge(START, "increment").addEdge("increment", END).compile() });
    await seedDefaultAssistants(runtime.store, runtime.listGraphs());
    const app = createApi(createPlatformAdapter(runtime, runtime.store, runtime.listGraphs()));
    const client = new Client({ apiUrl: "http://valida.test", apiKey: null,
      callerOptions: { maxRetries: 0,
        fetch: (input: RequestInfo | URL, init?: RequestInit) => app.fetch(new Request(input, init)) } });
    const thread = await client.threads.create();
    expect(await client.runs.wait(thread.thread_id, "counter", { input: { count: 10 } }))
      .toMatchObject({ count: 11 });
    const earlier = await client.threads.getState(thread.thread_id);
    const earlierId = earlier.checkpoint?.checkpoint_id;
    if (typeof earlierId !== "string") throw new Error("Missing earlier checkpoint ID");
    expect(await client.runs.wait(thread.thread_id, "counter", { input: {} }))
      .toMatchObject({ count: 12 });
    const oldHead = await client.threads.getState(thread.thread_id);
    const oldHeadId = oldHead.checkpoint?.checkpoint_id;
    if (typeof oldHeadId !== "string") throw new Error("Missing old head checkpoint ID");
    expect(oldHeadId).not.toBe(earlierId);

    const branch = await client.threads.updateState(thread.thread_id, {
      checkpointId: earlierId, values: { count: 40 },
    });
    const branchId = branch.configurable?.checkpoint_id;
    if (typeof branchId !== "string") throw new Error("Missing branch checkpoint ID");
    const branched = await client.threads.getState(thread.thread_id, branchId);
    expect(branched.values).toMatchObject({ count: 40 });
    expect(branched.parent_checkpoint?.checkpoint_id).toBe(earlierId);
    expect((await client.threads.getState(thread.thread_id, oldHeadId)).values)
      .toMatchObject({ count: 12 });
    expect((await client.threads.getHistory(thread.thread_id, { limit: 100 })).some(
      state => state.checkpoint?.checkpoint_id === branchId &&
        state.parent_checkpoint?.checkpoint_id === earlierId,
    )).toBe(true);
    expect(await client.runs.wait(thread.thread_id, "counter", { input: {} }))
      .toMatchObject({ count: 41 });

    const objectBranch = await client.threads.updateState(thread.thread_id, {
      checkpoint: { thread_id: thread.thread_id, checkpoint_id: oldHeadId,
        checkpoint_ns: "", checkpoint_map: {} }, values: { count: 70 },
    });
    const objectId = objectBranch.configurable?.checkpoint_id;
    if (typeof objectId !== "string") throw new Error("Missing object branch checkpoint ID");
    const objectState = await client.threads.getState(thread.thread_id, objectId);
    expect(objectState.values).toMatchObject({ count: 70 });
    expect(objectState.parent_checkpoint?.checkpoint_id).toBe(oldHeadId);
    expect(await client.runs.wait(thread.thread_id, "counter", { input: {} }))
      .toMatchObject({ count: 71 });

    const other = await client.threads.create();
    await expect(client.threads.updateState(other.thread_id, {
      checkpoint: { thread_id: thread.thread_id, checkpoint_id: objectId,
        checkpoint_ns: "", checkpoint_map: {} }, values: { count: 9 },
    })).rejects.toThrow();
  } finally {
    await runtime.close();
  }
});
