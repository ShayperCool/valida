import { expect, test } from "bun:test";
import { context, propagation, SpanKind, SpanStatusCode, trace } from "@opentelemetry/api";
import { InMemorySpanExporter } from "@opentelemetry/sdk-trace-base";
import { Hono } from "hono";
import { initializeTelemetryFromEnv, resolveTelemetryConfig, resolveTelemetryTargets, Telemetry, type TraceCarrier } from "./telemetry.ts";

test("OTLP environment selects a trace endpoint and protocol only when enabled", () => {
  expect(resolveTelemetryConfig({})).toBeNull();
  expect(initializeTelemetryFromEnv({ OTEL_TRACES_EXPORTER: "none" })).toBeNull();
  expect(resolveTelemetryConfig({
    OTEL_TRACES_EXPORTER: "console", OTEL_EXPORTER_OTLP_ENDPOINT: "https://collector.example",
  })).toBeNull();
  expect(resolveTelemetryConfig({
    OTEL_SERVICE_NAME: "valida-worker",
    OTEL_EXPORTER_OTLP_ENDPOINT: "https://collector.example/base/",
  })).toEqual({
    serviceName: "valida-worker", protocol: "http/protobuf",
    endpoint: "https://collector.example/base/v1/traces",
  });
  expect(resolveTelemetryConfig({
    OTEL_EXPORTER_OTLP_ENDPOINT: "https://wrong.example",
    OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: "https://collector.example/custom",
    OTEL_EXPORTER_OTLP_TRACES_PROTOCOL: "http/json",
  })).toEqual({
    serviceName: "valida", protocol: "http/json", endpoint: "https://collector.example/custom",
  });
  expect(resolveTelemetryConfig({ OTEL_TRACES_EXPORTER: "otlp", OTEL_EXPORTER_OTLP_PROTOCOL: "grpc" }))
    .toEqual({ serviceName: "valida", protocol: "grpc", endpoint: "http://localhost:4317" });
  expect(resolveTelemetryConfig({
    OTEL_EXPORTER_OTLP_ENDPOINT: "https://collector.example",
    OTEL_SDK_DISABLED: "true",
  })).toBeNull();
  expect(() => resolveTelemetryConfig({
    OTEL_EXPORTER_OTLP_ENDPOINT: "https://collector.example",
    OTEL_EXPORTER_OTLP_PROTOCOL: "unknown",
  })).toThrow("Unsupported OTLP trace protocol");
});

test("OTEL_TARGETS resolves generic, Langfuse, and Phoenix exporters", () => {
  const targets = resolveTelemetryTargets({
    OTEL_TARGETS: "LANGFUSE,PHOENIX,GENERIC",
    LANGFUSE_BASE_URL: "https://langfuse.example/",
    LANGFUSE_PUBLIC_KEY: "public",
    LANGFUSE_SECRET_KEY: "secret",
    PHOENIX_COLLECTOR_ENDPOINT: "https://phoenix.example/v1/traces",
    PHOENIX_API_KEY: "token",
    OTEL_EXPORTER_OTLP_ENDPOINT: "https://generic.example",
    OTEL_EXPORTER_OTLP_HEADERS: "X-Env=test",
  });
  expect(targets.map(target => target.endpoint)).toEqual([
    "https://langfuse.example/api/public/otel/v1/traces",
    "https://phoenix.example/v1/traces",
    "https://generic.example/v1/traces",
  ]);
  expect(targets[0]?.headers?.Authorization).toBe(`Basic ${Buffer.from("public:secret").toString("base64")}`);
  expect(targets[1]?.headers?.authorization).toBe("Bearer token");
  expect(targets[2]?.headers?.["X-Env"]).toBe("test");
  expect(resolveTelemetryTargets({ OTEL_TARGETS: "LANGFUSE" })).toEqual([]);
  expect(resolveTelemetryTargets({ OTEL_TARGETS: "GENERIC" })).toEqual([]);
  expect(() => resolveTelemetryTargets({ OTEL_TARGETS: "UNKNOWN" })).toThrow();
});

test("OTEL_TARGETS fans out the same span to two collectors", async () => {
  const received: string[][] = [[], []];
  const collectors = [0, 1].map(index => Bun.serve({ hostname: "127.0.0.1", port: 0,
    async fetch(request) {
      received[index]!.push(request.headers.get("content-type") ?? "");
      await request.arrayBuffer();
      return new Response(new Uint8Array(), { status: 200 });
    },
  }));
  const telemetry = initializeTelemetryFromEnv({
    OTEL_TARGETS: "PHOENIX,GENERIC",
    PHOENIX_COLLECTOR_ENDPOINT: `http://127.0.0.1:${collectors[0]!.port}/v1/traces`,
    OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: `http://127.0.0.1:${collectors[1]!.port}/v1/traces`,
  });
  if (!telemetry) throw new Error("Expected tracing to be enabled");
  try {
    await telemetry.withRunSpan({ graphId: "counter", runId: "fanout" }, async () => {});
    await telemetry.forceFlush();
    expect(received.map(items => items.length)).toEqual([1, 1]);
    expect(received.flat().every(value => value.includes("application/x-protobuf"))).toBe(true);
  } finally {
    await telemetry.shutdown();
    for (const collector of collectors) collector.stop(true);
    trace.disable();
    context.disable();
    propagation.disable();
  }
});

test("HTTP and graph run spans export in memory with inherited and queued trace context", async () => {
  const exporter = new InMemorySpanExporter();
  const telemetry = new Telemetry({
    exporter, serviceName: "valida-test", processor: "simple",
  });
  try {
    const app = new Hono();
    app.use("*", telemetry.middleware());
    let queuedContext: TraceCarrier = {};
    app.get("/threads/:threadId", async c => {
      queuedContext = telemetry.injectTraceContext();
      await telemetry.withRunSpan({
        graphId: "echo", runId: "run-inline", threadId: c.req.param("threadId"),
      }, async span => {
        span.setAttribute("valida.run.status", "success");
      });
      return c.json({ ok: true });
    });
    app.get("/failure", c => c.text("broken", 503));

    const traceId = "a".repeat(32);
    const parentId = "b".repeat(16);
    const response = await app.request("http://valida.test/threads/thread-1?token=secret", {
      headers: { traceparent: `00-${traceId}-${parentId}-01`, authorization: "Bearer secret" },
    });
    expect(response.status).toBe(200);
    expect(queuedContext.traceparent).toMatch(new RegExp(`^00-${traceId}-[0-9a-f]{16}-01$`));

    await telemetry.withRunSpan({
      graphId: "echo", runId: "run-worker", threadId: "thread-1",
      traceContext: queuedContext,
    }, async () => {});
    expect((await app.request("http://valida.test/failure")).status).toBe(503);
    await telemetry.forceFlush();

    const spans = exporter.getFinishedSpans();
    const server = spans.find(span => span.name === "GET /threads/:threadId");
    const inline = spans.find(span => span.attributes["valida.run.id"] === "run-inline");
    const worker = spans.find(span => span.attributes["valida.run.id"] === "run-worker");
    const failed = spans.find(span => span.name === "GET /failure");
    expect(server?.kind).toBe(SpanKind.SERVER);
    expect(server?.resource.attributes["service.name"]).toBe("valida-test");
    expect(server?.attributes).toMatchObject({
      "http.route": "/threads/:threadId",
      "http.request.method": "GET",
      "http.response.status_code": 200,
    });
    expect(server?.attributes).not.toHaveProperty("url.query");
    expect(server?.attributes).not.toHaveProperty("authorization");
    expect(server?.spanContext().traceId).toBe(traceId);
    expect(server?.parentSpanContext?.spanId).toBe(parentId);
    expect(inline?.parentSpanContext?.spanId).toBe(server?.spanContext().spanId);
    expect(worker?.parentSpanContext?.spanId).toBe(server?.spanContext().spanId);
    expect(inline?.attributes).toMatchObject({
      "valida.graph.id": "echo", "valida.thread.id": "thread-1", "valida.run.status": "success",
    });
    expect(failed?.status.code).toBe(SpanStatusCode.ERROR);
    expect(failed?.attributes["http.response.status_code"]).toBe(503);
  } finally {
    await telemetry.shutdown();
    trace.disable();
    context.disable();
    propagation.disable();
  }
});

test("graph failures set an error span and keep the original exception", async () => {
  const exporter = new InMemorySpanExporter();
  const telemetry = new Telemetry({ exporter, processor: "simple" });
  try {
    await expect(telemetry.withRunSpan({ graphId: "echo", runId: "failed" }, async () => {
      throw new Error("node failed");
    })).rejects.toThrow("node failed");
    const span = exporter.getFinishedSpans().find(item => item.attributes["valida.run.id"] === "failed");
    expect(span?.status.code).toBe(SpanStatusCode.ERROR);
    expect(span?.attributes["error.type"]).toBe("Error");
    expect(span?.events.some(event => event.name === "exception")).toBe(true);
  } finally {
    await telemetry.shutdown();
    trace.disable();
    context.disable();
    propagation.disable();
  }
});

test("OTLP HTTP exporter sends a run span to an in-process collector", async () => {
  const received: Array<{ path: string; contentType: string | null; body: unknown }> = [];
  const collector = Bun.serve({
    hostname: "127.0.0.1", port: 0,
    async fetch(request) {
      received.push({
        path: new URL(request.url).pathname,
        contentType: request.headers.get("content-type"),
        body: await request.json(),
      });
      return new Response(null, { status: 200 });
    },
  });
  const telemetry = initializeTelemetryFromEnv({
    OTEL_TRACES_EXPORTER: "otlp",
    OTEL_EXPORTER_OTLP_TRACES_PROTOCOL: "http/json",
    OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: `http://127.0.0.1:${collector.port}/v1/traces`,
    OTEL_SERVICE_NAME: "valida-collector-test",
  });
  if (!telemetry) throw new Error("OTLP telemetry should be enabled");
  try {
    await telemetry.withRunSpan({ graphId: "echo", runId: "otlp-run" }, async () => {});
    await telemetry.forceFlush();
    expect(received).toHaveLength(1);
    expect(received[0]?.path).toBe("/v1/traces");
    expect(received[0]?.contentType).toContain("application/json");
    expect(JSON.stringify(received[0]?.body)).toContain("valida.graph.run");
    expect(JSON.stringify(received[0]?.body)).toContain("valida-collector-test");
  } finally {
    await telemetry.shutdown();
    collector.stop(true);
    trace.disable();
    context.disable();
    propagation.disable();
  }
});

test("default OTLP protobuf exporter sends binary traces to an in-process collector", async () => {
  const received: Array<{ contentType: string | null; size: number }> = [];
  const collector = Bun.serve({
    hostname: "127.0.0.1", port: 0,
    async fetch(request) {
      received.push({
        contentType: request.headers.get("content-type"),
        size: (await request.arrayBuffer()).byteLength,
      });
      return new Response(new Uint8Array(), { status: 200 });
    },
  });
  const telemetry = initializeTelemetryFromEnv({
    OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: `http://127.0.0.1:${collector.port}/v1/traces`,
  });
  if (!telemetry) throw new Error("OTLP telemetry should be enabled");
  try {
    await telemetry.withRunSpan({ graphId: "echo", runId: "proto-run" }, async () => {});
    await telemetry.forceFlush();
    expect(received).toHaveLength(1);
    expect(received[0]?.contentType).toContain("application/x-protobuf");
    expect(received[0]?.size).toBeGreaterThan(0);
  } finally {
    await telemetry.shutdown();
    collector.stop(true);
    trace.disable();
    context.disable();
    propagation.disable();
  }
});
