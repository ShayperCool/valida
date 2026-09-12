import { expect, test } from "bun:test";
import { Client } from "@langchain/langgraph-sdk";
import { createApi } from "./index";
import { createRuntime } from "../engine/index";
import { createPlatformAdapter, seedDefaultAssistants } from "../platform";

test("LangGraph SDK runs a checkpointed HITL graph through the real API adapter", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite", url: ":memory:" } });
  try {
    runtime.registerGraph({
      id: "approval",
      entrypoint: "calculate",
      nodes: {
        calculate: (state) => ({ result: Number(state.input) * 2 }),
        approve: (_state, context) => ({ approved: context.interrupt({ question: "Approve?" }) }),
      },
      edges: { calculate: "approve", approve: "__end__" },
    });
    await seedDefaultAssistants(runtime.store, runtime.listGraphs());
    const app = createApi(createPlatformAdapter(runtime, runtime.store, runtime.listGraphs()));
    const client = new Client({
      apiUrl: "http://valida.test",
      apiKey: null,
      callerOptions: {
        maxRetries: 0,
        fetch: (input: RequestInfo | URL, init?: RequestInit) => app.fetch(new Request(input, init)),
      },
    });
    const thread = await client.threads.create();
    const interrupted = await client.runs.wait(thread.thread_id, "approval", { input: { input: 4 } });
    expect(interrupted).toMatchObject({ input: 4, result: 8 });
    const state = await client.threads.getState(thread.thread_id);
    expect(state.checkpoint?.checkpoint_id).toBeTruthy();
    expect(state.next).toContain("approve");
    expect((state as typeof state & { interrupts: unknown[] }).interrupts).toHaveLength(1);
    const resumed = await client.runs.wait(thread.thread_id, "approval", { command: { resume: true } });
    expect(resumed).toMatchObject({ input: 4, result: 8, approved: true });
    expect((await client.threads.getHistory(thread.thread_id)).length).toBeGreaterThan(2);

    const stream = client.threads.stream({
      assistantId: "approval",
      maxReconnectAttempts: 0,
      streamIdleReconnect: 0,
      fetch: ((input: RequestInfo | URL, init?: RequestInit) => app.fetch(new Request(input, init))) as typeof fetch,
    });
    try {
      await stream.run.start({ input: { input: 5 } });
      expect(await stream.values).toMatchObject({ input: 5, result: 10 });
      expect(stream.interrupts).toHaveLength(1);
      await stream.input.respond({
        interrupt_id: stream.interrupts[0]!.interruptId,
        namespace: [],
        response: true,
      });
      const resumedRun = (await client.runs.list(stream.threadId))[0]!;
      await client.runs.join(stream.threadId, resumedRun.run_id);
      expect((await client.threads.getState(stream.threadId)).values).toMatchObject({
        input: 5, result: 10, approved: true,
      });
    } finally {
      await stream.close();
    }
  } finally {
    await runtime.close();
  }
});
