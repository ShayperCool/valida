import { Hono } from "hono";
import { EventType, RunAgentInputSchema, type BaseEvent, type RunAgentInput } from "@ag-ui/core";
import { EventEncoder } from "@ag-ui/encoder";
import type { ProtocolEvent } from "@langchain/langgraph/stream";
import { currentAuthorization } from "../auth.ts";
import type { NativeV2Runtime } from "./native_v2.ts";
import { ApiError, type ApiRequestContext, type JsonRecord, type PlatformAdapter, type Run, type ThreadState } from "./types.ts";

type ApiEnv = { Variables: { principal: unknown } };
type Event = BaseEvent;

const object = (value: unknown): JsonRecord =>
  value !== null && typeof value === "object" && !Array.isArray(value) ? value as JsonRecord : {};
const text = (value: unknown): string => {
  if (typeof value === "string") return value;
  if (Array.isArray(value)) return value.map(part => typeof part === "string" ? part : String(object(part).text ?? "")).join("");
  return "";
};
const statusError = (status: number, message: string) =>
  new Response(JSON.stringify({ detail: message }), { status, headers: { "content-type": "application/json" } });

function stateWithoutMessages(value: unknown): JsonRecord {
  const { messages: _messages, ...state } = object(value);
  return state;
}

function newMessages(input: RunAgentInput, previous: ThreadState | null): JsonRecord[] {
  const stored = Array.isArray(previous?.values.messages) ? previous.values.messages : [];
  const latestStoredId = [...stored].reverse().map(message => object(message).id).find(id => typeof id === "string");
  const matched = latestStoredId ? input.messages.findIndex(message => message.id === latestStoredId) : -1;
  const incoming = stored.length === 0 ? input.messages
    : matched >= 0 ? input.messages.slice(matched + 1)
    : input.messages.filter(message => message.role === "user").slice(-1);
  return incoming.filter(message => ["user", "assistant", "system", "developer"].includes(message.role))
    .map(message => ({ id: message.id,
      role: message.role === "user" ? "human" : message.role === "assistant" ? "ai" : "system",
      content: text("content" in message ? message.content : "") }));
}

function interruptsOf(state: ThreadState | null): Array<{ id: string; reason: string; message?: string; metadata: JsonRecord }> {
  if (!state) return [];
  return state.interrupts.map((raw, index) => {
    const entry = object(raw);
    const value = entry.value ?? raw;
    const payload = object(value);
    const action = Array.isArray(payload.action_requests) ? object(payload.action_requests[0]) : {};
    return {
      id: typeof entry.id === "string" ? entry.id : `${state.checkpoint?.checkpoint_id ?? "checkpoint"}:${index}`,
      reason: "human_input",
      message: typeof value === "string" ? value
        : typeof action.description === "string" ? action.description
        : typeof payload.message === "string" ? payload.message : undefined,
      metadata: { value },
    };
  });
}

function resumeValue(input: RunAgentInput, state: ThreadState | null): unknown {
  const pending = interruptsOf(state);
  if (pending.length !== 1 || input.resume?.length !== 1) {
    throw new ApiError(422, "AG-UI resume currently requires exactly one pending interrupt");
  }
  const response = input.resume[0]!;
  if (response.interruptId !== pending[0]!.id) throw new ApiError(422, "Unknown interruptId");
  if (response.status !== "resolved") throw new ApiError(422, "Cancelled AG-UI interrupts are not supported");
  return response.payload;
}

function messageEvents(message: JsonRecord): Event[] {
  const role = message.type ?? message.role;
  if (role !== "ai" && role !== "assistant") return [];
  const content = text(message.content);
  if (!content) return [];
  const messageId = typeof message.id === "string" ? message.id : crypto.randomUUID();
  return [
    { type: EventType.TEXT_MESSAGE_START, messageId, role: "assistant" },
    { type: EventType.TEXT_MESSAGE_CONTENT, messageId, delta: content },
    { type: EventType.TEXT_MESSAGE_END, messageId },
  ];
}

function toolEvents(message: JsonRecord): Event[] {
  const role = message.type ?? message.role;
  if (role === "tool") {
    const toolCallId = message.tool_call_id;
    if (typeof toolCallId !== "string") return [];
    return [{ type: EventType.TOOL_CALL_RESULT, toolCallId,
      messageId: typeof message.id === "string" ? message.id : crypto.randomUUID(),
      content: text(message.content) }];
  }
  if (role !== "ai" && role !== "assistant") return [];
  const calls = Array.isArray(message.tool_calls) ? message.tool_calls : [];
  return calls.flatMap(raw => {
    const call = object(raw);
    if (typeof call.id !== "string" || typeof call.name !== "string") return [];
    return [
      { type: EventType.TOOL_CALL_START, toolCallId: call.id, toolCallName: call.name,
        parentMessageId: typeof message.id === "string" ? message.id : undefined },
      { type: EventType.TOOL_CALL_ARGS, toolCallId: call.id, delta: JSON.stringify(call.args ?? {}) },
      { type: EventType.TOOL_CALL_END, toolCallId: call.id },
    ];
  });
}

function nativeEvents(event: ProtocolEvent, messages: Map<string, { id: string; started: boolean }>): Event[] {
  if (event.method === "values" && event.params.namespace.length === 0) {
    return [{ type: EventType.STATE_SNAPSHOT, snapshot: stateWithoutMessages(event.params.data) }];
  }
  if (event.method === "updates" && event.params.namespace.length === 0) {
    const values = object(object(event.params.data).values);
    return (Array.isArray(values.messages) ? values.messages : []).flatMap(raw => toolEvents(object(raw)));
  }
  if (event.method !== "messages") return [];
  const data = object(event.params.data);
  const key = event.params.namespace.join("|");
  if (data.event === "message-start") {
    const id = String(data.id ?? data.run_id ?? crypto.randomUUID());
    messages.set(key, { id, started: true });
    return [{ type: EventType.TEXT_MESSAGE_START, messageId: id, role: "assistant" }];
  }
  const active = messages.get(key);
  if (!active) return [];
  if (data.event === "content-block-delta") {
    const delta = object(data.delta);
    return delta.type === "text-delta" && typeof delta.text === "string" && delta.text.length > 0
      ? [{ type: EventType.TEXT_MESSAGE_CONTENT, messageId: active.id, delta: delta.text }] : [];
  }
  if (data.event === "message-finish") {
    messages.delete(key);
    return [{ type: EventType.TEXT_MESSAGE_END, messageId: active.id }];
  }
  return [];
}

async function *projectRun(
  adapter: PlatformAdapter, runtime: NativeV2Runtime, run: Run,
  input: RunAgentInput, context: ApiRequestContext,
): AsyncGenerator<Event> {
  const messages = new Map<string, { id: string; started: boolean }>();
  const emittedIds = new Set<string>();
  const assistant = await adapter.assistants.get(run.assistant_id, context);
  if (runtime.supportsV2(assistant?.graph_id ?? run.assistant_id)) {
    for await (const item of runtime.streamV2(run.run_id, { signal: context.request.signal })) {
      for (const event of nativeEvents(item.event, messages)) {
        if (event.type === EventType.TEXT_MESSAGE_START && typeof event.messageId === "string") {
          emittedIds.add(event.messageId);
        }
        yield event;
      }
    }
  } else {
    for await (const item of adapter.runs.events(run.thread_id, run.run_id, null, context)) {
      if (item.event === "values") yield { type: EventType.STATE_SNAPSHOT, snapshot: stateWithoutMessages(item.data) };
    }
  }
  const finished = await adapter.runs.get(run.thread_id, run.run_id, context);
  if (!finished) throw new ApiError(404, `Run '${run.run_id}' not found`);
  if (finished.status === "error" || finished.status === "timeout") {
    yield { type: EventType.RUN_ERROR, message: `Run ${finished.run_id} ${finished.status}` };
    return;
  }
  const state = await adapter.threads.getState(run.thread_id, null, context);
  const values = state?.values ?? object(await adapter.runs.join(run.thread_id, run.run_id, context));
  for (const raw of Array.isArray(values.messages) ? values.messages : []) {
    const message = object(raw);
    if (typeof message.id === "string" && !emittedIds.has(message.id) &&
      !input.messages.some(original => original.id === message.id)) {
      for (const event of messageEvents(message)) yield event;
    }
  }
  yield { type: EventType.STATE_SNAPSHOT, snapshot: stateWithoutMessages(values) };
  const pending = finished.status === "interrupted" ? interruptsOf(state) : [];
  yield { type: EventType.RUN_FINISHED, threadId: input.threadId, runId: input.runId,
    result: values, outcome: pending.length ? { type: "interrupt", interrupts: pending } : { type: "success" } };
}

/** POST /ag-ui/:assistantId accepts the official AG-UI HttpAgent input and streams its events. */
export function createAgUiApi(adapter: PlatformAdapter, runtime: NativeV2Runtime): Hono<ApiEnv> {
  const app = new Hono<ApiEnv>();
  app.onError(cause => cause instanceof ApiError
    ? statusError(cause.status, cause.message) : (console.error(cause), statusError(500, "Internal server error")));
  app.post("/ag-ui/:assistantId", async c => {
    let parsed: unknown;
    try { parsed = await c.req.json(); } catch { throw new ApiError(400, "Invalid JSON body"); }
    const validated = RunAgentInputSchema.safeParse(currentAuthorization.getStore()?.payload ?? parsed);
    if (!validated.success) throw new ApiError(422, "Invalid AG-UI RunAgentInput");
    const input = validated.data;
    const context: ApiRequestContext = { request: c.req.raw, principal: c.get("principal") };
    const assistantId = c.req.param("assistantId");
    if (!await adapter.assistants.get(assistantId, context)) throw new ApiError(404, `Assistant '${assistantId}' not found`);
    const thread = await adapter.threads.get(input.threadId, context)
      ?? await adapter.threads.create({ thread_id: input.threadId }, context);
    const previous = await adapter.threads.getState(thread.thread_id, null, context);
    const command = input.resume?.length ? { resume: resumeValue(input, previous) } : undefined;
    const forwarded = object(input.forwardedProps);
    const payload: JsonRecord = { assistant_id: assistantId,
      ...(command ? { command } : { input: { ...stateWithoutMessages(input.state),
        messages: newMessages(input, previous) } }),
      config: object(forwarded.config), metadata: object(forwarded.metadata) };
    const run = await adapter.runs.create(thread.thread_id, payload, context);
    const encoder = new EventEncoder({ accept: c.req.header("accept") });
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        void (async () => {
          try {
            const started: Event = { type: EventType.RUN_STARTED, threadId: input.threadId,
              runId: input.runId, ...(input.parentRunId ? { parentRunId: input.parentRunId } : {}) };
            controller.enqueue(encoder.encodeBinary(started));
            for await (const event of projectRun(adapter, runtime, run, input, context)) {
              controller.enqueue(encoder.encodeBinary(event));
            }
          } catch (cause) {
            try { controller.enqueue(encoder.encodeBinary({ type: EventType.RUN_ERROR,
              message: cause instanceof Error ? cause.message : String(cause) })); } catch { /* Client closed. */ }
          } finally {
            try { controller.close(); } catch { /* Client closed. */ }
          }
        })();
      },
      cancel() { /* Stream consumer disconnected; the durable run can still finish. */ },
    });
    return new Response(stream, { headers: { "content-type": encoder.getContentType(),
      "cache-control": "no-cache, no-transform", "x-accel-buffering": "no" } });
  });
  return app;
}
