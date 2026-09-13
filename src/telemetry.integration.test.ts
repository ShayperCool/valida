import { expect, test } from "bun:test";
import { context, propagation, trace } from "@opentelemetry/api";
import { InMemorySpanExporter } from "@opentelemetry/sdk-trace-base";
import { Hono } from "hono";
import { createApi } from "./api/index.ts";
import { createRuntime } from "./engine/index.ts";
import { createPlatformAdapter, seedDefaultAssistants } from "./platform.ts";
import { Telemetry } from "./telemetry.ts";

test("HTTP request context reaches the actual graph run span", async () => {
  const exporter = new InMemorySpanExporter();
  const telemetry = new Telemetry({ exporter, processor: "simple", serviceName: "valida-integration" });
  const runtime = await createRuntime({ db: { dialect: "sqlite", url: ":memory:" }, telemetry });
  try {
    runtime.registerGraph({ id: "counter", entrypoint: "add", nodes: {
      add: state => ({ count: Number(state.count ?? 0) + 1 }),
    } });
    await seedDefaultAssistants(runtime.store, runtime.listGraphs());
    const app = new Hono();
    app.use("*", telemetry.middleware());
    app.route("/", createApi(createPlatformAdapter(runtime, runtime.store, runtime.listGraphs())));
    const created = await app.request("/threads", { method: "POST",
      headers: { "content-type": "application/json" }, body: "{}" });
    const thread = await created.json() as { thread_id: string };
    const traceId = "a".repeat(32);
    const response = await app.request(`/threads/${thread.thread_id}/runs/wait`, {
      method: "POST",
      headers: { "content-type": "application/json",
        traceparent: `00-${traceId}-${"b".repeat(16)}-01` },
      body: JSON.stringify({ assistant_id: "counter", input: { count: 4 } }),
    });
    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({ count: 5 });
    await telemetry.forceFlush();
    const spans = exporter.getFinishedSpans();
    const requestSpan = spans.find(span => span.name === "POST /threads/:threadId/runs/wait");
    const runSpan = spans.find(span => span.name === "valida.graph.run");
    expect(requestSpan?.spanContext().traceId).toBe(traceId);
    expect(runSpan?.spanContext().traceId).toBe(traceId);
    expect(runSpan?.parentSpanContext?.spanId).toBe(requestSpan?.spanContext().spanId);
    expect(runSpan?.attributes).toMatchObject({
      "valida.graph.id": "counter", "valida.run.status": "success",
    });
  } finally {
    await runtime.close();
    await telemetry.shutdown();
    trace.disable();
    context.disable();
    propagation.disable();
  }
});
