import { context, propagation, SpanKind, SpanStatusCode, type Span, type Tracer } from "@opentelemetry/api";
import { OTLPTraceExporter as GrpcTraceExporter } from "@opentelemetry/exporter-trace-otlp-grpc";
import { OTLPTraceExporter as JsonTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
import { OTLPTraceExporter as ProtoTraceExporter } from "@opentelemetry/exporter-trace-otlp-proto";
import { resourceFromAttributes } from "@opentelemetry/resources";
import { BatchSpanProcessor, ConsoleSpanExporter, SimpleSpanProcessor, type SpanExporter } from "@opentelemetry/sdk-trace-base";
import { NodeTracerProvider } from "@opentelemetry/sdk-trace-node";
import type { MiddlewareHandler } from "hono";
import { matchedRoutes } from "hono/route";

type Environment = Record<string, string | undefined>;

export type OtlpProtocol = "grpc" | "http/protobuf" | "http/json";
export interface TelemetryConfig {
  serviceName: string;
  protocol: OtlpProtocol;
  endpoint: string;
}

interface ExportTarget extends TelemetryConfig {
  headers?: Record<string, string>;
}

export interface TraceCarrier {
  traceparent?: string;
  tracestate?: string;
}

export interface RunSpanInfo {
  graphId: string;
  runId: string;
  threadId?: string | null;
  assistantId?: string | null;
  /** Store this carrier in queued run metadata to continue a request trace in another worker. */
  traceContext?: TraceCarrier;
}

/** Optional tracing is enabled only when an OTLP endpoint or exporter is configured. */
export function resolveTelemetryConfig(env: Environment = process.env): TelemetryConfig | null {
  if (env.OTEL_SDK_DISABLED?.toLowerCase() === "true") return null;
  const exporter = env.OTEL_TRACES_EXPORTER?.split(",").map(value => value.trim().toLowerCase());
  if (exporter && !exporter.includes("otlp")) return null;
  const explicitEndpoint = env.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT;
  const baseEndpoint = env.OTEL_EXPORTER_OTLP_ENDPOINT;
  if (!explicitEndpoint && !baseEndpoint && !exporter?.includes("otlp")) return null;

  const protocol = env.OTEL_EXPORTER_OTLP_TRACES_PROTOCOL ?? env.OTEL_EXPORTER_OTLP_PROTOCOL ?? "http/protobuf";
  if (!["grpc", "http/protobuf", "http/json"].includes(protocol)) {
    throw new Error(`Unsupported OTLP trace protocol: ${protocol}`);
  }
  const endpoint = explicitEndpoint ?? (baseEndpoint
    ? protocol === "grpc" ? baseEndpoint : `${baseEndpoint.replace(/\/+$/, "")}/v1/traces`
    : protocol === "grpc" ? "http://localhost:4317" : "http://localhost:4318/v1/traces");
  try {
    new URL(endpoint);
  } catch {
    throw new Error(`Invalid OTLP trace endpoint: ${endpoint}`);
  }
  return {
    serviceName: env.OTEL_SERVICE_NAME?.trim() || "valida",
    protocol: protocol as OtlpProtocol,
    endpoint,
  };
}

function otlpExporter(config: ExportTarget): SpanExporter {
  const options = { url: config.endpoint, headers: config.headers };
  if (config.protocol === "grpc") return new GrpcTraceExporter(options);
  if (config.protocol === "http/json") return new JsonTraceExporter(options);
  return new ProtoTraceExporter(options);
}

function parseHeaders(raw?: string): Record<string, string> {
  return Object.fromEntries((raw ?? "").split(",").flatMap(part => {
    const at = part.indexOf("=");
    return at > 0 ? [[part.slice(0, at).trim(), part.slice(at + 1).trim()]] : [];
  }));
}

/** Resolves Aegra-compatible OTEL_TARGETS; each target receives the same spans. */
export function resolveTelemetryTargets(env: Environment = process.env): ExportTarget[] {
  if (env.OTEL_SDK_DISABLED?.toLowerCase() === "true" || env.OTEL_TRACES_EXPORTER?.toLowerCase() === "none") return [];
  const named = env.OTEL_TARGETS?.split(",").map(value => value.trim().toUpperCase()).filter(Boolean);
  const selected = named?.length ? named : ["GENERIC"];
  const serviceName = env.OTEL_SERVICE_NAME?.trim() || "valida";
  const targets: ExportTarget[] = [];
  for (const name of new Set(selected)) {
    if (name === "GENERIC" || name === "DEFAULT" || name === "OTLP") {
      if (named?.length && !env.OTEL_EXPORTER_OTLP_ENDPOINT && !env.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT) continue;
      const config = resolveTelemetryConfig({ ...env, OTEL_TRACES_EXPORTER: "otlp" });
      if (config) targets.push({ ...config, headers: parseHeaders(env.OTEL_EXPORTER_OTLP_HEADERS) });
    } else if (name === "LANGFUSE") {
      if (!env.LANGFUSE_PUBLIC_KEY || !env.LANGFUSE_SECRET_KEY) continue;
      const base = (env.LANGFUSE_BASE_URL || "http://localhost:3000").replace(/\/+$/, "");
      targets.push({ serviceName, protocol: "http/protobuf", endpoint: `${base}/api/public/otel/v1/traces`,
        headers: { Authorization: `Basic ${Buffer.from(`${env.LANGFUSE_PUBLIC_KEY}:${env.LANGFUSE_SECRET_KEY}`).toString("base64")}`,
          "x-langfuse-ingestion-version": "4" } });
    } else if (name === "PHOENIX") {
      targets.push({ serviceName, protocol: "http/protobuf",
        endpoint: env.PHOENIX_COLLECTOR_ENDPOINT || "http://127.0.0.1:6006/v1/traces",
        headers: env.PHOENIX_API_KEY ? { authorization: `Bearer ${env.PHOENIX_API_KEY}` } : {} });
    } else throw new Error(`Unsupported OTEL_TARGETS entry: ${name}`);
  }
  for (const target of targets) new URL(target.endpoint);
  return targets;
}

function exception(span: Span, cause: unknown): void {
  const error = cause instanceof Error ? cause : new Error(String(cause));
  span.recordException(error);
  span.setStatus({ code: SpanStatusCode.ERROR, message: error.message });
  span.setAttribute("error.type", error.name);
}

export class Telemetry {
  readonly provider: NodeTracerProvider;
  readonly tracer: Tracer;

  constructor(options: {
    exporter: SpanExporter | readonly SpanExporter[];
    serviceName?: string;
    registerGlobal?: boolean;
    /** Simple processing is useful for deterministic tests; production uses batches. */
    processor?: "simple" | "batch";
  }) {
    const exporters = Array.isArray(options.exporter) ? options.exporter : [options.exporter];
    this.provider = new NodeTracerProvider({
      resource: resourceFromAttributes({ "service.name": options.serviceName ?? "valida" }),
      spanProcessors: exporters.map(exporter => options.processor === "simple"
        ? new SimpleSpanProcessor(exporter)
        : new BatchSpanProcessor(exporter)),
    });
    if (options.registerGlobal !== false) this.provider.register();
    this.tracer = this.provider.getTracer("valida", "0.1.0");
  }

  /** Hono middleware. Register it before the API routes. */
  middleware(): MiddlewareHandler {
    return async (hono, next) => {
      const request = hono.req.raw;
      const url = new URL(request.url);
      const method = request.method.toUpperCase();
      const incoming: TraceCarrier = {
        traceparent: request.headers.get("traceparent") ?? undefined,
        tracestate: request.headers.get("tracestate") ?? undefined,
      };
      const parent = propagation.extract(context.active(), incoming);
      return context.with(parent, () => this.tracer.startActiveSpan(method, {
        kind: SpanKind.SERVER,
        attributes: {
          "http.request.method": method,
          "url.path": url.pathname,
          "url.scheme": url.protocol.slice(0, -1),
          "server.address": url.hostname,
          ...(url.port ? { "server.port": Number(url.port) } : {}),
        },
      }, async span => {
        try {
          await next();
          const route = matchedRoutes(hono).at(-1)?.path;
          if (route && route !== "*") {
            span.updateName(`${method} ${route}`);
            span.setAttribute("http.route", route);
          }
          span.setAttribute("http.response.status_code", hono.res.status);
          if (hono.res.status >= 500) {
            span.setStatus({ code: SpanStatusCode.ERROR });
            span.setAttribute("error.type", String(hono.res.status));
          }
        } catch (cause) {
          exception(span, cause);
          throw cause;
        } finally {
          span.end();
        }
      }));
    };
  }

  /** Capture W3C trace context before placing work on BullMQ or another queue. */
  injectTraceContext(): TraceCarrier {
    const carrier: Record<string, string> = {};
    propagation.inject(context.active(), carrier);
    return { traceparent: carrier.traceparent, tracestate: carrier.tracestate };
  }

  /** Wrap graph execution, optionally continuing a trace from a queued request. */
  withRunSpan<T>(info: RunSpanInfo, work: (span: Span) => T | Promise<T>): Promise<T> {
    const parent = info.traceContext
      ? propagation.extract(context.active(), info.traceContext)
      : context.active();
    return context.with(parent, () => this.tracer.startActiveSpan("valida.graph.run", {
      kind: SpanKind.INTERNAL,
      attributes: {
        "valida.graph.id": info.graphId,
        "valida.run.id": info.runId,
        ...(info.threadId ? { "valida.thread.id": info.threadId } : {}),
        ...(info.assistantId ? { "valida.assistant.id": info.assistantId } : {}),
      },
    }, async span => {
      try {
        return await work(span);
      } catch (cause) {
        exception(span, cause);
        throw cause;
      } finally {
        span.end();
      }
    }));
  }

  forceFlush(): Promise<void> { return this.provider.forceFlush(); }
  shutdown(): Promise<void> { return this.provider.shutdown(); }
}

export function initializeTelemetryFromEnv(env: Environment = process.env): Telemetry | null {
  const targets = resolveTelemetryTargets(env);
  const consoleEnabled = env.OTEL_SDK_DISABLED?.toLowerCase() !== "true" &&
    env.OTEL_TRACES_EXPORTER?.toLowerCase() !== "none" &&
    (env.OTEL_CONSOLE_EXPORT?.toLowerCase() === "true" ||
      env.OTEL_TRACES_EXPORTER?.split(",").some(value => value.trim().toLowerCase() === "console"));
  const exporters: SpanExporter[] = targets.map(otlpExporter);
  if (consoleEnabled) exporters.push(new ConsoleSpanExporter());
  if (exporters.length === 0) return null;
  return new Telemetry({
    exporter: exporters,
    serviceName: targets[0]?.serviceName ?? env.OTEL_SERVICE_NAME?.trim() ?? "valida",
  });
}
