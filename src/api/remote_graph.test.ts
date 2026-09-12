import { expect, test } from "bun:test";
import { Annotation, Command, END, interrupt, START, StateGraph } from "@langchain/langgraph";
import { RemoteGraph } from "@langchain/langgraph/remote";
import { Client } from "@langchain/langgraph-sdk";
import { createApi } from "./index.ts";
import { createRuntime } from "../engine/index.ts";
import { createPlatformAdapter, seedDefaultAssistants } from "../platform.ts";

/** Exercises the published RemoteGraph client against the real Hono adapter. */
test("RemoteGraph invokes and streams stateless and stateful deterministic runs", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite", url: ":memory:" } });
  try {
    const schema = Annotation.Root({ count: Annotation<number> });
    const graph = new StateGraph(schema)
      .addNode("increment", state => ({ count: (state.count ?? 0) + 1 }))
      .addEdge(START, "increment")
      .addEdge("increment", END)
      .compile();
    runtime.registerGraph({ id: "counter", graph });
    await seedDefaultAssistants(runtime.store, runtime.listGraphs());
    const app = createApi(createPlatformAdapter(runtime, runtime.store, runtime.listGraphs()));
    const requests: string[] = [];
    const client = new Client({
      apiUrl: "http://valida.test",
      apiKey: null,
      callerOptions: {
        maxRetries: 0,
        fetch: (input: RequestInfo | URL, init?: RequestInit) => {
          requests.push(`${init?.method ?? "GET"} ${new URL(String(input)).pathname}`);
          return app.fetch(new Request(input, init));
        },
      },
    });
    const remote = new RemoteGraph({ graphId: "counter", client });

    expect(await remote.invoke({ count: 1 })).toMatchObject({ count: 2 });
    const statelessChunks = [];
    for await (const chunk of await remote.stream({ count: 4 }, { streamMode: "values" })) {
      statelessChunks.push(chunk);
    }
    expect(statelessChunks.at(-1)).toMatchObject({ count: 5 });
    expect(requests.filter(path => path === "POST /runs/stream")).toHaveLength(2);

    const thread = await client.threads.create();
    const config = { configurable: { thread_id: thread.thread_id } };
    expect(await remote.invoke({ count: 10 }, config)).toMatchObject({ count: 11 });
    const firstState = await remote.getState(config);
    expect(firstState.values).toMatchObject({ count: 11 });
    expect(firstState.config.configurable?.checkpoint_id).toBeTruthy();

    const statefulChunks = [];
    for await (const chunk of await remote.stream({}, { ...config, streamMode: "values" })) {
      statefulChunks.push(chunk);
    }
    expect(statefulChunks.at(-1)).toMatchObject({ count: 12 });
    expect((await remote.getState(config)).values).toMatchObject({ count: 12 });
    const history = [];
    for await (const snapshot of remote.getStateHistory(config)) history.push(snapshot);
    expect(history.length).toBeGreaterThan(2);
    expect(history[0]?.config.configurable?.checkpoint_id).toBeTruthy();
    expect(requests.filter(path => path === `POST /threads/${thread.thread_id}/runs/stream`)).toHaveLength(2);

    const parent = new StateGraph(schema)
      .addNode("remote", remote)
      .addEdge(START, "remote")
      .addEdge("remote", END)
      .compile();
    expect(await parent.invoke({ count: 20 })).toMatchObject({ count: 21 });
  } finally {
    await runtime.close();
  }
});

test("RemoteGraph resumes an interrupted stateful graph from its checkpoint", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite", url: ":memory:" } });
  try {
    const schema = Annotation.Root({ proposal: Annotation<string>, accepted: Annotation<boolean> });
    const graph = new StateGraph(schema)
      .addNode("review", () => ({ accepted: interrupt("Approve proposal?") as boolean }))
      .addEdge(START, "review")
      .addEdge("review", END)
      .compile();
    runtime.registerGraph({ id: "review", graph });
    await seedDefaultAssistants(runtime.store, runtime.listGraphs());
    const app = createApi(createPlatformAdapter(runtime, runtime.store, runtime.listGraphs()));
    const client = new Client({
      apiUrl: "http://valida.test", apiKey: null,
      callerOptions: {
        maxRetries: 0,
        fetch: (input: RequestInfo | URL, init?: RequestInit) => app.fetch(new Request(input, init)),
      },
    });
    const remote = new RemoteGraph({ graphId: "review", client });
    const thread = await client.threads.create();
    const config = { configurable: { thread_id: thread.thread_id } };
    await expect(remote.invoke({ proposal: "ship" }, config)).rejects.toThrow();
    const interrupted = await remote.getState(config);
    expect(interrupted.values).toMatchObject({ proposal: "ship" });
    expect(interrupted.next).toContain("review");
    expect(interrupted.config.configurable?.checkpoint_id).toBeTruthy();

    expect(await remote.invoke(new Command({ resume: true }), config)).toMatchObject({
      proposal: "ship", accepted: true,
    });
    expect((await remote.getState(config)).next).toEqual([]);
  } finally {
    await runtime.close();
  }
});
