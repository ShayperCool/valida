import { context, propagation, SpanKind, SpanStatusCode, type Span, type Tracer } from "@opentelemetry/api";
import { OTLPTraceExporter as GrpcTraceExporter } from "@opentelemetry/exporter-trace-otlp-grpc";
import { OTLPTraceExporter as JsonTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
import { OTLPTraceExporter as ProtoTraceExporter } from "@opentelemetry/exporter-trace-otlp-proto";
import { resourceFromAttributes } from "@opentelemetry/resources";
import { BatchSpanProcessor, SimpleSpanProcessor, type SpanExporter } from "@opentelemetry/sdk-trace-base";
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

function otlpExporter(config: TelemetryConfig): SpanExporter {
  const options = { url: config.endpoint };
  if (config.protocol === "grpc") return new GrpcTraceExporter(options);
  if (config.protocol === "http/json") return new JsonTraceExporter(options);
  return new ProtoTraceExporter(options);
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
    exporter: SpanExporter;
    serviceName?: string;
    registerGlobal?: boolean;
    /** Simple processing is useful for deterministic tests; production uses batches. */
    processor?: "simple" | "batch";
  }) {
    this.provider = new NodeTracerProvider({
      resource: resourceFromAttributes({ "service.name": options.serviceName ?? "valida" }),
      spanProcessors: [options.processor === "simple"
        ? new SimpleSpanProcessor(options.exporter)
        : new BatchSpanProcessor(options.exporter)],
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
  const config = resolveTelemetryConfig(env);
  if (!config) return null;
  return new Telemetry({
    exporter: otlpExporter(config),
    serviceName: config.serviceName,
  });
}
