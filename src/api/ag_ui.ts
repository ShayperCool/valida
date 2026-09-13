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

function stateEvents(previous: JsonRecord | null, next: JsonRecord): Event[] {
  if (previous === null) return [{ type: EventType.STATE_SNAPSHOT, snapshot: next }];
  const delta: JsonRecord[] = [];
  for (const key of new Set([...Object.keys(previous), ...Object.keys(next)])) {
    const path = `/${key.replaceAll("~", "~0").replaceAll("/", "~1")}`;
    if (!(key in next)) delta.push({ op: "remove", path });
    else if (!(key in previous)) delta.push({ op: "add", path, value: next[key] });
    else if (JSON.stringify(previous[key]) !== JSON.stringify(next[key])) {
      delta.push({ op: "replace", path, value: next[key] });
    }
  }
  return delta.length ? [{ type: EventType.STATE_DELTA, delta }] : [];
}

function newMessages(input: RunAgentInput, previous: ThreadState | null): JsonRecord[] {
  const stored = Array.isArray(previous?.values.messages) ? previous.values.messages : [];
  const storedIds = new Set(stored.map(message => object(message).id).filter((id): id is string => typeof id === "string"));
  const latestStoredId = [...stored].reverse().map(message => object(message).id).find(id => typeof id === "string");
  const matched = latestStoredId ? input.messages.findIndex(message => message.id === latestStoredId) : -1;
  const incoming = stored.length === 0 ? input.messages
    : matched >= 0 ? input.messages.slice(matched + 1)
    : input.messages.filter(message => !storedIds.has(message.id) && (message.role === "user" || message.role === "tool"));
  return incoming.flatMap(message => {
    if (message.role === "tool") return [{ id: message.id, role: "tool",
      content: message.content, tool_call_id: message.toolCallId }];
    if (!["user", "assistant", "system", "developer"].includes(message.role)) return [];
    const result: JsonRecord = { id: message.id,
      role: message.role === "user" ? "human" : message.role === "assistant" ? "ai" : "system",
      content: "content" in message ? message.content ?? "" : "" };
    if (message.role === "assistant" && message.toolCalls?.length) {
      result.tool_calls = message.toolCalls.map(call => {
        let args: unknown;
        try { args = JSON.parse(call.function.arguments); }
        catch { args = call.function.arguments; }
        return { id: call.id, name: call.function.name, args };
      });
    }
    return [result];
  });
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
  const responses = input.resume ?? [];
  if (pending.length === 0 || responses.length !== pending.length) {
    throw new ApiError(422, "AG-UI resume must address every pending interrupt");
  }
  const pendingIds = new Set(pending.map(interrupt => interrupt.id));
  const byId = new Map(responses.map(response => [response.interruptId, response]));
  if (byId.size !== pendingIds.size || responses.some(response => !pendingIds.has(response.interruptId))) {
    throw new ApiError(422, "AG-UI resume has an unknown or duplicate interruptId");
  }
  if (responses.some(response => response.status === "cancelled" && response.payload !== undefined)) {
    throw new ApiError(422, "Cancelled AG-UI resume must omit payload");
  }
  if (responses.length === 1) {
    const response = responses[0]!;
    return response.status === "cancelled"
      ? { __agui_cancelled__: true, interrupt_id: response.interruptId }
      : response.payload;
  }
  return { __agui_resume_map__: Object.fromEntries(responses.map(response => [response.interruptId,
    { status: response.status, ...(response.status === "resolved" ? { payload: response.payload } : {}) }])) };
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

function agUiMessages(values: JsonRecord): JsonRecord[] {
  return (Array.isArray(values.messages) ? values.messages : []).flatMap(raw => {
    const message = object(raw);
    const id = message.id;
    if (typeof id !== "string") return [];
    const role = message.type ?? message.role;
    if (role === "human" || role === "user") return [{ id, role: "user", content: message.content ?? "" }];
    if (role === "ai" || role === "assistant") return [{ id, role: "assistant", content: text(message.content),
      ...(Array.isArray(message.tool_calls) ? { toolCalls: message.tool_calls.map(rawCall => {
        const call = object(rawCall);
        return { id: call.id, type: "function", function: { name: call.name,
          arguments: JSON.stringify(call.args ?? {}) } };
      }) } : {}) }];
    if (role === "tool") return [{ id, role: "tool", content: text(message.content),
      toolCallId: message.tool_call_id }];
    if (role === "system" || role === "developer") return [{ id, role, content: text(message.content) }];
    return [];
  });
}

type ActiveMessage = { id: string; textStarted: boolean; reasoningStarted: boolean };

function nativeEvents(event: ProtocolEvent, messages: Map<string, ActiveMessage>): Event[] {
  if (event.method === "updates" && event.params.namespace.length === 0) {
    const values = object(object(event.params.data).values);
    return (Array.isArray(values.messages) ? values.messages : []).flatMap(raw => toolEvents(object(raw)));
  }
  if (event.method !== "messages") return [];
  const data = object(event.params.data);
  const key = event.params.namespace.join("|");
  if (data.event === "message-start") {
    if (data.role && data.role !== "ai") return [];
    const id = String(data.id ?? data.run_id ?? crypto.randomUUID());
    messages.set(key, { id, textStarted: false, reasoningStarted: false });
    return [];
  }
  const active = messages.get(key);
  if (!active) return [];
  if (data.event === "content-block-delta") {
    const delta = object(data.delta);
    if (delta.type === "text-delta" && typeof delta.text === "string" && delta.text.length > 0) {
      const start: Event[] = active.textStarted ? []
        : [{ type: EventType.TEXT_MESSAGE_START, messageId: active.id, role: "assistant" }];
      active.textStarted = true;
      return [...start, { type: EventType.TEXT_MESSAGE_CONTENT, messageId: active.id, delta: delta.text }];
    }
    if (delta.type === "reasoning-delta" && typeof delta.reasoning === "string" && delta.reasoning.length > 0) {
      const messageId = `${active.id}:reasoning`;
      const start: Event[] = active.reasoningStarted ? [] : [
        { type: EventType.REASONING_START, messageId },
        { type: EventType.REASONING_MESSAGE_START, messageId, role: "reasoning" },
      ];
      active.reasoningStarted = true;
      return [...start, { type: EventType.REASONING_MESSAGE_CONTENT, messageId, delta: delta.reasoning }];
    }
    return [];
  }
  if (data.event === "message-finish") {
    messages.delete(key);
    return [
      ...(active.reasoningStarted ? [
        { type: EventType.REASONING_MESSAGE_END, messageId: `${active.id}:reasoning` },
        { type: EventType.REASONING_END, messageId: `${active.id}:reasoning` },
      ] : []),
      ...(active.textStarted ? [{ type: EventType.TEXT_MESSAGE_END, messageId: active.id }] : []),
    ];
  }
  return [];
}

async function *projectRun(
  adapter: PlatformAdapter, runtime: NativeV2Runtime, run: Run,
  input: RunAgentInput, previous: ThreadState | null, context: ApiRequestContext,
): AsyncGenerator<Event> {
  const messages = new Map<string, ActiveMessage>();
  let projectedState: JsonRecord | null = null;
  const emittedIds = new Set<string>();
  const previousIds = new Set((Array.isArray(previous?.values.messages) ? previous.values.messages : [])
    .map(message => object(message).id).filter((id): id is string => typeof id === "string"));
  const assistant = await adapter.assistants.get(run.assistant_id, context);
  if (runtime.supportsV2(assistant?.graph_id ?? run.assistant_id)) {
    for await (const item of runtime.streamV2(run.run_id, { signal: context.request.signal })) {
      if (item.event.method === "values" && item.event.params.namespace.length === 0) {
        const next = stateWithoutMessages(item.event.params.data);
        for (const event of stateEvents(projectedState, next)) yield event;
        projectedState = next;
      }
      for (const event of nativeEvents(item.event, messages)) {
        if (event.type === EventType.TEXT_MESSAGE_START && typeof event.messageId === "string") {
          emittedIds.add(event.messageId);
        }
        yield event;
      }
    }
  } else {
    for await (const item of adapter.runs.events(run.thread_id, run.run_id, null, context)) {
      if (item.event === "values") {
        const next = stateWithoutMessages(item.data);
        for (const event of stateEvents(projectedState, next)) yield event;
        projectedState = next;
      }
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
    if (typeof message.id === "string" && !previousIds.has(message.id) && !emittedIds.has(message.id) &&
      !input.messages.some(original => original.id === message.id)) {
      for (const event of messageEvents(message)) yield event;
    }
  }
  const pending = finished.status === "interrupted" ? interruptsOf(state) : [];
  const finalState = stateWithoutMessages(values);
  if (pending.length) {
    yield { type: EventType.STATE_SNAPSHOT, snapshot: finalState };
    yield { type: EventType.MESSAGES_SNAPSHOT, messages: agUiMessages(values) };
  } else {
    for (const event of stateEvents(projectedState, finalState)) yield event;
  }
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
    if (interruptsOf(previous).length && !input.resume?.length) {
      throw new ApiError(422, "Pending AG-UI interrupts require resume entries");
    }
    const command = input.resume?.length ? { resume: resumeValue(input, previous) } : undefined;
    const forwarded = object(input.forwardedProps);
    const forwardedConfig = object(forwarded.config);
    const payload: JsonRecord = { assistant_id: assistantId,
      ...(command ? { command } : { input: { ...stateWithoutMessages(input.state),
        messages: newMessages(input, previous) } }),
      config: { ...forwardedConfig, configurable: { ...object(forwardedConfig.configurable),
        ag_ui_tools: input.tools, ag_ui_context: input.context } },
      metadata: object(forwarded.metadata) };
    const run = await adapter.runs.create(thread.thread_id, payload, context);
    const encoder = new EventEncoder({ accept: c.req.header("accept") });
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        void (async () => {
          try {
            const started: Event = { type: EventType.RUN_STARTED, threadId: input.threadId,
              runId: input.runId, ...(input.parentRunId ? { parentRunId: input.parentRunId } : {}) };
            controller.enqueue(encoder.encodeBinary(started));
            for await (const event of projectRun(adapter, runtime, run, input, previous, context)) {
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
