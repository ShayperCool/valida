import { expect, test } from "bun:test";
import { Hono } from "hono";
import { Client } from "@langchain/langgraph-sdk";
import { Annotation, END, START, StateGraph } from "@langchain/langgraph";
import { authMiddleware } from "../auth.ts";
import { createRuntime } from "../engine/index.ts";
import { createPlatformAdapter, seedDefaultAssistants } from "../platform.ts";
import { createApi } from "./index.ts";

test("SDK createBatch starts ordered stateless runs and authorizes every item", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite", url: ":memory:" } });
  try {
    const schema = Annotation.Root({ count: Annotation<number> });
    const graph = new StateGraph(schema)
      .addNode("increment", state => ({ count: state.count + 1 }))
      .addEdge(START, "increment").addEdge("increment", END).compile();
    runtime.registerGraph({ id: "counter", graph });
    await seedDefaultAssistants(runtime.store, runtime.listGraphs());
    const adapter = createPlatformAdapter(runtime, runtime.store, runtime.listGraphs());
    const create = adapter.runs.create;
    let createdCount = 0;
    adapter.runs.create = (...args) => {
      createdCount++;
      return create(...args);
    };
    const authorizedInputs: number[] = [];
    const app = new Hono();
    app.use("*", authMiddleware({
      authenticate(request) {
        if (request.headers.get("authorization") !== "Bearer valid") throw new Error("Invalid token");
        return { identity: "alice" };
      },
      authorize(context, value) {
        expect(context.resource).toBe("threads");
        expect(context.action).toBe("create_run");
        const count = (value.input as { count?: number } | undefined)?.count;
        if (typeof count === "number") authorizedInputs.push(count);
        if (count === -1) return false;
        if (count === 5) return { ...value, input: { count: 50 } };
        return true;
      },
    }));
    app.route("/", createApi(adapter));
    const client = new Client({ apiUrl: "http://valida.test", apiKey: null,
      defaultHeaders: { authorization: "Bearer valid" },
      callerOptions: { maxRetries: 0,
        fetch: ((input: RequestInfo | URL, init?: RequestInit) =>
          app.fetch(new Request(input, init))) as typeof fetch } });

    await expect(client.runs.createBatch([
      { assistantId: "counter", input: { count: 1 } },
      { assistantId: "counter", input: { count: -1 } },
    ])).rejects.toThrow("HTTP 403");
    expect(createdCount).toBe(0);

    const runs = await client.runs.createBatch([
      { assistantId: "counter", input: { count: 1 } },
      { assistantId: "counter", input: { count: 5 } },
    ]);
    expect(runs).toHaveLength(2);
    expect(new Set(runs.map(run => run.thread_id)).size).toBe(2);
    expect(authorizedInputs).toEqual([1, -1, 1, 5]);
    expect((await runtime.waitRun(runs[0]!.run_id)).output).toMatchObject({ count: 2 });
    expect((await runtime.waitRun(runs[1]!.run_id)).output).toMatchObject({ count: 51 });
    expect((await runtime.store.getThread(runs[0]!.thread_id))?.metadata._owner).toBe("alice");

    const invalid = await app.request("/runs/batch", {
      method: "POST", headers: { authorization: "Bearer valid", "content-type": "application/json" },
      body: JSON.stringify([
        { assistant_id: "counter", input: { count: 3 } },
        { assistant_id: "counter", input: { count: 4 }, thread_id: runs[0]!.thread_id },
      ]),
    });
    expect(invalid.status).toBe(422);
    expect(createdCount).toBe(2);
  } finally {
    await runtime.close();
  }
});
