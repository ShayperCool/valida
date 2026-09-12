import type { ApiRequestContext, JsonRecord, PlatformAdapter, Run, StreamEvent } from "./types";
import { ApiError } from "./types";

const object = (value: unknown): JsonRecord | null =>
  value !== null && typeof value === "object" && !Array.isArray(value) ? value as JsonRecord : null;

function protocolError(id: number | null, error: string, message: string): JsonRecord {
  return { type: "error", id, error, message };
}

function protocolSuccess(id: number, result: JsonRecord): JsonRecord {
  return { type: "success", id, result, meta: { applied_through_seq: 0 } };
}

/**
 * Compatibility bridge for the SDK's HTTP v2 transport. It projects persisted
 * legacy run events; native content-block messages, tools and subgraph events
 * require a runtime-supplied `adapter.v2` implementation.
 */
export class LegacyV2Bridge {
  constructor(private readonly adapter: PlatformAdapter) {}

  async command(threadId: string, payload: JsonRecord, context: ApiRequestContext): Promise<JsonRecord> {
    const id = typeof payload.id === "number" && Number.isInteger(payload.id) ? payload.id : null;
    if (id === null || typeof payload.method !== "string" || !object(payload.params)) {
      return protocolError(id, "invalid_argument", "Commands require an integer id, string method and object params.");
    }
    const params = payload.params as JsonRecord;
    try {
      if (payload.method === "run.start") {
        const assistantId = params.assistant_id;
        if (typeof assistantId !== "string" || !assistantId) {
          return protocolError(id, "invalid_argument", "run.start requires assistant_id.");
        }
        const existing = await this.adapter.threads.get(threadId, context);
        if (!existing) await this.adapter.threads.create({ thread_id: threadId }, context);
        const runPayload: JsonRecord = { ...params, assistant_id: assistantId };
        if (existing?.status === "interrupted" && params.input != null) {
          delete runPayload.input;
          runPayload.command = { resume: params.input };
        }
        if (runPayload.multitaskStrategy) {
          runPayload.multitask_strategy = runPayload.multitaskStrategy;
          delete runPayload.multitaskStrategy;
        }
        const run = await this.adapter.runs.create(threadId, runPayload, context);
        return protocolSuccess(id, { run_id: run.run_id });
      }
      if (payload.method === "input.respond") {
        const runs = await this.adapter.runs.list(threadId, { limit: 1, offset: 0 }, context);
        const assistantId = typeof params.assistant_id === "string"
          ? params.assistant_id
          : runs[0]?.assistant_id;
        if (!assistantId) return protocolError(id, "no_such_run", "No run on this thread to resume.");
        let resume: unknown;
        if (Array.isArray(params.responses)) {
          resume = Object.fromEntries(params.responses
            .filter((entry) => object(entry) && typeof entry.interrupt_id === "string")
            .map((entry) => [entry.interrupt_id, entry.response]));
        } else if ("response" in params) {
          resume = params.response;
        } else {
          return protocolError(id, "invalid_argument", "input.respond requires response or responses.");
        }
        const command: JsonRecord = { resume };
        if (params.update !== undefined) command.update = params.update;
        if (params.goto !== undefined) command.goto = params.goto;
        const run = await this.adapter.runs.create(
          threadId,
          { assistant_id: assistantId, command, config: params.config, metadata: params.metadata },
          context,
        );
        return protocolSuccess(id, { run_id: run.run_id });
      }
      if (payload.method === "state.get") {
        const state = await this.adapter.threads.getState(threadId, null, context);
        return state ? protocolSuccess(id, { state }) : protocolError(id, "no_such_run", "Thread not found.");
      }
      return protocolError(id, "unknown_command", `Unknown command '${payload.method}'.`);
    } catch (cause) {
      if (cause instanceof ApiError) {
        const code = cause.status === 403 ? "permission_denied"
          : cause.status === 404 ? "no_such_run" : "invalid_argument";
        return protocolError(id, code, cause.message);
      }
      return protocolError(id, "unknown_error", cause instanceof Error ? cause.message : String(cause));
    }
  }

  async *events(threadId: string, payload: JsonRecord, context: ApiRequestContext): AsyncIterable<StreamEvent> {
    const channels = Array.isArray(payload.channels)
      ? new Set(payload.channels.filter((entry): entry is string => typeof entry === "string"))
      : new Set<string>();
    if (channels.size === 0) throw new ApiError(400, "channels must be a non-empty array");
    const seen = new Set<string>();
    let seq = 0;
    const since = typeof payload.since === "number" && Number.isInteger(payload.since) ? payload.since : 0;
    const emit = (method: string, data: unknown, eventId: string): StreamEvent | null => {
      seq += 1;
      const channel = method === "input.requested" ? "input" : method;
      if (!channels.has(channel) && !(channel === "custom" && channels.has("custom"))) return null;
      if (seq <= since) return null;
      const envelope = {
        type: "event", seq, method,
        params: { data, namespace: [], timestamp: Date.now() },
        event_id: eventId,
      };
      return { id: String(seq), event: method, data: envelope };
    };
    while (!context.request.signal.aborted) {
      const runs = await this.adapter.runs.list(threadId, { limit: 100_000, offset: 0 }, context);
      runs.sort((a, b) => a.created_at.localeCompare(b.created_at));
      for (const run of runs) {
        if (seen.has(run.run_id)) continue;
        seen.add(run.run_id);
        const started = emit("lifecycle", { event: "running", graph_name: run.assistant_id }, `${run.run_id}:running`);
        if (started) yield started;
        yield* this.projectRun(run, context, emit);
      }
      await new Promise<void>((resolve) => setTimeout(resolve, 50));
    }
  }

  private async *projectRun(
    run: Run,
    context: ApiRequestContext,
    emit: (method: string, data: unknown, eventId: string) => StreamEvent | null,
  ): AsyncIterable<StreamEvent> {
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
        const event = emit("lifecycle", { event: status, graph_name: run.assistant_id }, eventId);
        if (event) yield event;
        return;
      }
      if (raw.event === "updates") {
        const interrupts = value?.__interrupt__;
        if (Array.isArray(interrupts)) {
          for (let i = 0; i < interrupts.length; i++) {
            const entry = object(interrupts[i]);
            const interruptId = typeof entry?.id === "string" ? entry.id : `${run.run_id}:${i}`;
            const data = { interrupt_id: interruptId, payload: entry?.value, value: entry?.value };
            const event = emit("input.requested", data, `${eventId}:interrupt:${i}`);
            if (event) yield event;
          }
        }
        const entries = Object.entries(value ?? {}).filter(([key]) => key !== "__interrupt__");
        if (entries.length > 0) {
          const data = entries.length === 1
            ? { node: entries[0]![0], values: object(entries[0]![1]) ?? { value: entries[0]![1] } }
            : { values: Object.fromEntries(entries) };
          const event = emit("updates", data, eventId);
          if (event) yield event;
        }
        continue;
      }
      const method = raw.event.startsWith("custom:") ? "custom" : raw.event;
      const data = method === "custom"
        ? { name: raw.event.slice("custom:".length), payload: raw.data }
        : raw.data;
      const event = emit(method, data, eventId);
      if (event) yield event;
    }
    const final = emit("lifecycle", { event: "completed", graph_name: run.assistant_id }, `${run.run_id}:end`);
    if (final) yield final;
  }
}
