import { matchesSubscription, type ProtocolEvent } from "@langchain/langgraph/stream";
import type { SubscribeParams } from "@langchain/protocol";
import type { ApiRequestContext, JsonRecord, PlatformAdapter, Run, StreamEvent } from "./types";
import { ApiError } from "./types";
import { LegacyV2Bridge } from "./v2";

/** The small part of GraphRuntime needed to serve native protocol events. */
export interface NativeV2Runtime {
  supportsV2(graphId: string): boolean;
  streamV2(
    runId: string,
    options?: { after?: number; signal?: AbortSignal },
  ): AsyncIterable<{ id: string; event: ProtocolEvent }>;
}

const object = (value: unknown): JsonRecord | null =>
  value !== null && typeof value === "object" && !Array.isArray(value) ? value as JsonRecord : null;

/**
 * Serves compiled LangGraph events as native protocol envelopes, while custom
 * Valida graphs continue to use the legacy event projection.
 */
export class NativeV2Bridge {
  private readonly legacy: LegacyV2Bridge;

  constructor(private readonly adapter: PlatformAdapter, private readonly runtime: NativeV2Runtime) {
    this.legacy = new LegacyV2Bridge(adapter);
  }

  command(threadId: string, payload: JsonRecord, context: ApiRequestContext): Promise<JsonRecord> {
    return this.legacy.command(threadId, payload, context);
  }

  async *events(threadId: string, payload: JsonRecord, context: ApiRequestContext): AsyncIterable<StreamEvent> {
    if (!Array.isArray(payload.channels) || payload.channels.length === 0 ||
      !payload.channels.every((channel) => typeof channel === "string")) {
      throw new ApiError(400, "channels must be a non-empty array of strings");
    }
    const filter = payload as unknown as SubscribeParams;
    const since = typeof payload.since === "number" && Number.isInteger(payload.since) ? payload.since : 0;
    const seen = new Set<string>();
    let seq = 0;
    const emit = (source: ProtocolEvent, eventId: string): StreamEvent | null => {
      const event = { ...source, seq: ++seq, event_id: eventId };
      if (event.seq <= since || !matchesSubscription(event, filter)) return null;
      return { id: String(event.seq), event: event.method, data: event };
    };
    while (!context.request.signal.aborted) {
      const runs = await this.adapter.runs.list(threadId, { limit: 100_000, offset: 0 }, context);
      runs.sort((a, b) => a.created_at.localeCompare(b.created_at) || a.run_id.localeCompare(b.run_id));
      for (const run of runs) {
        if (seen.has(run.run_id)) continue;
        seen.add(run.run_id);
        const assistant = await this.adapter.assistants.get(run.assistant_id, context);
        const graphId = assistant?.graph_id ?? run.assistant_id;
        if (this.runtime.supportsV2(graphId)) {
          for await (const item of this.runtime.streamV2(run.run_id, { signal: context.request.signal })) {
            const output = emit(item.event, `${run.run_id}:${item.id}`);
            if (output) yield output;
          }
        } else {
          yield* this.projectLegacy(run, context, emit);
        }
      }
      await new Promise<void>((resolve) => setTimeout(resolve, 50));
    }
  }

  private async *projectLegacy(
    run: Run,
    context: ApiRequestContext,
    emit: (event: ProtocolEvent, eventId: string) => StreamEvent | null,
  ): AsyncIterable<StreamEvent> {
    const wrap = (method: string, data: unknown, eventId: string): StreamEvent | null => emit({
      type: "event", seq: 0, method,
      params: { data, namespace: [], timestamp: Date.now() },
    }, eventId);
    const started = wrap("lifecycle", { event: "running", graph_name: run.assistant_id }, `${run.run_id}:running`);
    if (started) yield started;
    let index = 0;
    for await (const raw of this.adapter.runs.events(run.thread_id, run.run_id, null, context)) {
      index += 1;
      const eventId = `${run.run_id}:${raw.id ?? index}`;
      const value = object(raw.data);
      if (raw.event === "metadata") continue;
      if (raw.event === "end" || raw.event === "error") {
        const status = raw.event === "error" ? "failed"
          : value?.status === "interrupted" ? "interrupted"
          : value?.status === "error" || value?.status === "timeout" ? "failed" : "completed";
        const terminal = wrap("lifecycle", { event: status, graph_name: run.assistant_id }, eventId);
        if (terminal) yield terminal;
        return;
      }
      if (raw.event === "updates") {
        const interrupts = value?.__interrupt__;
        if (Array.isArray(interrupts)) {
          for (let i = 0; i < interrupts.length; i++) {
            const entry = object(interrupts[i]);
            const interruptId = typeof entry?.id === "string" ? entry.id : `${run.run_id}:${i}`;
            const event = wrap("input.requested", {
              interrupt_id: interruptId, payload: entry?.value, value: entry?.value,
            }, `${eventId}:interrupt:${i}`);
            if (event) yield event;
          }
        }
        const entries = Object.entries(value ?? {}).filter(([key]) => key !== "__interrupt__");
        if (entries.length > 0) {
          const data = entries.length === 1
            ? { node: entries[0]![0], values: object(entries[0]![1]) ?? { value: entries[0]![1] } }
            : { values: Object.fromEntries(entries) };
          const event = wrap("updates", data, eventId);
          if (event) yield event;
        }
        continue;
      }
      const method = raw.event.startsWith("custom:") ? "custom" : raw.event;
      const data = method === "custom"
        ? { name: raw.event.slice("custom:".length), payload: raw.data }
        : raw.data;
      const event = wrap(method, data, eventId);
      if (event) yield event;
    }
    const completed = wrap("lifecycle", { event: "completed", graph_name: run.assistant_id }, `${run.run_id}:end`);
    if (completed) yield completed;
  }
}
