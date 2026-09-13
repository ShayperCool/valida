import type { ProtocolEvent } from "@langchain/langgraph/stream";
import type { ApiRequestContext, JsonRecord, PlatformAdapter, Run, StreamEvent } from "./types";
import { ApiError } from "./types";
import type { NativeV2Runtime } from "./native_v2";

type Mode = "values" | "updates" | "messages" | "messages-tuple" | "custom" | "events" | "debug" |
  "tasks" | "checkpoints";
type Part = Pick<StreamEvent, "event" | "data">;
type Cursor = { source: number; part: number };

const validModes = new Set<Mode>([
  "values", "updates", "messages", "messages-tuple", "custom", "events", "debug", "tasks", "checkpoints",
]);
const object = (value: unknown): JsonRecord | null =>
  value !== null && typeof value === "object" && !Array.isArray(value) ? value as JsonRecord : null;

function modesOf(value: unknown): Set<Mode> {
  let raw: unknown = value;
  if (raw == null) raw = ["values"];
  else if (typeof raw === "string") {
    if (raw.startsWith("[")) {
      try { raw = JSON.parse(raw); } catch { throw new ApiError(422, "Unsupported stream_mode"); }
    } else raw = raw.split(",").map(mode => mode.trim());
  }
  if (!Array.isArray(raw) || raw.length === 0 || !raw.every(item => typeof item === "string" && validModes.has(item as Mode))) {
    throw new ApiError(422, "Unsupported stream_mode");
  }
  return new Set(raw as Mode[]);
}

function cursorOf(value: string | null | undefined): Cursor {
  if (!value) return { source: 0, part: -1 };
  const match = /^(\d+)(?:\.(\d+))?$/.exec(value);
  return match ? { source: Number(match[1]), part: match[2] === undefined ? Number.POSITIVE_INFINITY : Number(match[2]) }
    : { source: 0, part: -1 };
}

function afterCursor(source: number, part: number, cursor: Cursor): boolean {
  return source > cursor.source || (source === cursor.source && part > cursor.part);
}

function withNamespace(event: string, namespace: readonly string[], subgraphs: boolean): string {
  return subgraphs && namespace.length > 0 ? `${event}|${namespace.join("|")}` : event;
}

interface MessageState {
  id: string;
  role: string;
  namespace: string[];
  node?: string;
  metadata: JsonRecord;
  blocks: JsonRecord[];
  deltas: number;
}

function messageContent(state: MessageState): unknown {
  return state.blocks.length === 1 && state.blocks[0]?.type === "text"
    ? state.blocks[0].text ?? "" : state.blocks;
}

function messageWire(state: MessageState, type = state.role): JsonRecord {
  return { type, id: state.id, content: messageContent(state), additional_kwargs: {}, response_metadata: {} };
}

function updateBlock(state: MessageState, index: number, delta: JsonRecord): void {
  const block = { ...(state.blocks[index] ?? { type: "text", text: "" }) };
  if (delta.type === "block-delta") Object.assign(block, object(delta.fields) ?? {});
  else if (delta.type === "text-delta") block.text = String(block.text ?? "") + String(delta.text ?? "");
  else if (delta.type === "reasoning-delta") block.reasoning = String(block.reasoning ?? "") + String(delta.reasoning ?? "");
  else if (delta.type === "data-delta") block.data = String(block.data ?? "") + String(delta.data ?? "");
  state.blocks[index] = block;
}

/** Original astream_events callback ancestry is not present in v3; this is a useful v1 trace projection. */
function rawTrace(event: ProtocolEvent, runId: string): JsonRecord {
  const data = object(event.params.data) ?? {};
  const signal = data.event;
  const node = event.params.node ?? event.params.namespace.at(-1)?.split(":", 1)[0] ?? "root";
  let kind = "on_chain_stream";
  if (event.method === "messages") {
    kind = signal === "message-start" ? "on_chat_model_start"
      : signal === "message-finish" ? "on_chat_model_end" : "on_chat_model_stream";
  } else if (event.method === "tasks") {
    kind = "result" in data ? "on_chain_end" : "on_chain_start";
  } else if (event.method === "lifecycle") {
    kind = signal === "running" || signal === "started" ? "on_chain_start"
      : signal === "completed" || signal === "failed" ? "on_chain_end" : "on_chain_stream";
  }
  const payload = event.method === "messages" && signal === "content-block-delta"
    ? { chunk: { type: "ai", content: object(data.delta)?.text ?? data.delta } }
    : event.method === "updates" ? { chunk: ["updates", data] } : { chunk: event.params.data };
  return { event: kind, name: node, run_id: String(data.run_id ?? runId), tags: [],
    metadata: { langgraph_node: node, langgraph_checkpoint_ns: event.params.namespace.join("|") },
    data: payload, parent_ids: [] };
}

/** Projects durable v2 events to the v1 SSE modes used by runs.stream/joinStream. */
export class V1StreamBridge {
  constructor(private readonly adapter: PlatformAdapter, private readonly runtime: NativeV2Runtime) {}

  async *events(
    threadId: string | null, run: Run, body: JsonRecord, context: ApiRequestContext,
    lastEventId: string | null = null,
  ): AsyncIterable<StreamEvent> {
    const modes = modesOf(body.stream_mode);
    const assistant = await this.adapter.assistants.get(run.assistant_id, context);
    const graphId = assistant?.graph_id ?? run.assistant_id;
    if (this.runtime.supportsV2(graphId)) {
      yield* this.native(run, modes, body.stream_subgraphs === true, context, cursorOf(lastEventId));
    } else {
      yield* this.legacy(threadId, run, modes, context, cursorOf(lastEventId));
    }
  }

  private async *native(
    run: Run, modes: Set<Mode>, subgraphs: boolean, context: ApiRequestContext, cursor: Cursor,
  ): AsyncIterable<StreamEvent> {
    if (afterCursor(0, 0, cursor)) {
      yield { id: "0.0", event: "metadata", data: { run_id: run.run_id, thread_id: run.thread_id, attempt: 1 } };
    }
    const messages = new Map<string, MessageState>();
    let lastSource = 0;
    for await (const item of this.runtime.streamV2(run.run_id, {
      // Rebuild message accumulation before applying the SSE cursor.
      after: 0, signal: context.request.signal,
    })) {
      const source = Number(item.id);
      if (!Number.isSafeInteger(source)) continue;
      lastSource = source;
      const parts = await this.projectNative(item.event, run, modes, subgraphs, messages, context);
      for (let index = 0; index < parts.length; index++) {
        if (afterCursor(source, index, cursor)) yield { ...parts[index]!, id: `${source}.${index}` };
      }
    }
    const final = await this.adapter.runs.get(run.thread_id, run.run_id, context);
    const status = final?.status ?? run.status;
    const source = lastSource + 1;
    if (status === "error" || status === "timeout") {
      if (afterCursor(source, 0, cursor)) {
        yield { id: `${source}.0`, event: "error", data: { message: `Run ${run.run_id} ${status}` } };
      }
    }
    if (afterCursor(source, 1, cursor)) yield { id: `${source}.1`, event: "end", data: { status } };
  }

  private async projectNative(
    event: ProtocolEvent, run: Run, modes: Set<Mode>, subgraphs: boolean,
    messages: Map<string, MessageState>, context: ApiRequestContext,
  ): Promise<Part[]> {
    const result: Part[] = [];
    const { method, params } = event;
    const data = object(params.data) ?? {};
    const namespace = params.namespace;
    const named = (mode: string, value: unknown, prefix = true) => result.push({
      event: prefix ? withNamespace(mode, namespace, subgraphs) : mode, data: value,
    });
    const rootVisible = subgraphs || namespace.length === 0;
    if (method === "values" && modes.has("values") && rootVisible) named("values", params.data);
    if (method === "updates" && rootVisible) {
      const update = typeof data.node === "string" ? { [data.node]: data.values } : params.data;
      if (modes.has("updates")) named("updates", update);
      else if (Array.isArray(object(update)?.__interrupt__)) named("values", update);
    }
    if ((method === "custom" || method.startsWith("custom:")) && modes.has("custom")) {
      named("custom", method === "custom" && "payload" in data ? data.payload : params.data);
    }
    if (method === "messages" && (modes.has("messages") || modes.has("messages-tuple"))) {
      this.projectMessage(event, data, modes, subgraphs, messages, result);
    }
    if (method === "checkpoints" && rootVisible) {
      if (modes.has("checkpoints")) named("checkpoints", params.data);
      if (modes.has("debug")) {
        const state = typeof data.id === "string"
          ? await this.adapter.threads.getState(run.thread_id, data.id, context) : null;
        named("debug", { type: "checkpoint", timestamp: new Date(params.timestamp).toISOString(),
          step: data.step, payload: state ? {
            config: { configurable: state.checkpoint }, values: state.values, metadata: state.metadata,
            next: state.next, tasks: state.tasks, parent_config: state.parent_checkpoint
              ? { configurable: state.parent_checkpoint } : null,
          } : params.data });
      }
    }
    if (method === "tasks" && rootVisible) {
      if (modes.has("tasks")) named("tasks", params.data);
      if (modes.has("debug")) named("debug", { type: "result" in data ? "task_result" : "task",
        timestamp: new Date(params.timestamp).toISOString(), payload: params.data });
    }
    if (method === "input.requested" && !modes.has("updates")) {
      named("values", { __interrupt__: [params.data] }, false);
    }
    if (modes.has("events")) named("events", rawTrace(event, run.run_id), false);
    return result;
  }

  private projectMessage(
    event: ProtocolEvent, data: JsonRecord, modes: Set<Mode>, subgraphs: boolean,
    messages: Map<string, MessageState>, output: Part[],
  ): void {
    const key = event.params.namespace.join("|");
    if (data.event === "message-start") {
      const id = String(data.id ?? data.run_id ?? `${key}:${event.seq}`);
      const metadata = { ...(object(data.metadata) ?? {}),
        ...(data.run_id ? { run_id: data.run_id } : {}),
        ...(event.params.node ? { langgraph_node: event.params.node } : {}),
        langgraph_checkpoint_ns: key };
      messages.set(key, { id, role: typeof data.role === "string" ? data.role : "ai",
        namespace: [...event.params.namespace], node: event.params.node, metadata, blocks: [], deltas: 0 });
      if (!modes.has("messages-tuple")) output.push({ event: "messages/metadata",
        data: { [id]: { metadata } } });
      return;
    }
    const state = messages.get(key);
    if (!state) return;
    const index = typeof data.index === "number" ? data.index : 0;
    if (data.event === "content-block-start") {
      state.blocks[index] = object(data.content) ?? { type: "text", text: "" };
    } else if (data.event === "content-block-delta") {
      const delta = object(data.delta) ?? {};
      updateBlock(state, index, delta);
      state.deltas++;
      if (modes.has("messages-tuple")) {
        const content = delta.type === "text-delta" ? delta.text ?? "" : [delta];
        output.push({ event: withNamespace("messages", state.namespace, subgraphs),
          data: [{ type: "AIMessageChunk", id: state.id, content }, state.metadata] });
      } else {
        output.push({ event: "messages/partial", data: [messageWire(state)] });
      }
    } else if (data.event === "content-block-finish") {
      state.blocks[index] = object(data.content) ?? state.blocks[index] ?? {};
    } else if (data.event === "message-finish") {
      if (modes.has("messages-tuple")) {
        if (state.deltas === 0) output.push({ event: withNamespace("messages", state.namespace, subgraphs),
          data: [{ type: "AIMessageChunk", id: state.id, content: messageContent(state) }, state.metadata] });
      } else {
        output.push({ event: "messages/complete", data: [messageWire(state)] });
      }
      messages.delete(key);
    }
  }

  private async *legacy(
    threadId: string | null, run: Run, modes: Set<Mode>, context: ApiRequestContext, cursor: Cursor,
  ): AsyncIterable<StreamEvent> {
    const lastEventId = cursor.source > 0 ? String(cursor.source - 1) : null;
    for await (const raw of this.adapter.runs.events(threadId, run.run_id, lastEventId, context)) {
      const source = Number(raw.id);
      if (!Number.isSafeInteger(source)) continue;
      const parts: Part[] = [];
      const data = object(raw.data) ?? {};
      if (["metadata", "error"].includes(raw.event)) parts.push(raw);
      else if (raw.event === "values" && modes.has("values")) parts.push(raw);
      else if (raw.event === "updates") {
        if (modes.has("updates")) parts.push(raw);
        else if (Array.isArray(data.__interrupt__)) parts.push({ event: "values", data: raw.data });
      } else if (raw.event.startsWith("custom") && modes.has("custom")) {
        parts.push({ event: "custom", data: raw.event.startsWith("custom:") ? raw.data : data.payload ?? raw.data });
      } else if (["messages", "messages/partial", "messages/complete", "messages/metadata"].includes(raw.event) &&
        (modes.has("messages") || modes.has("messages-tuple"))) parts.push(raw);
      else if (["tasks", "checkpoints", "debug"].includes(raw.event) && modes.has(raw.event as Mode)) parts.push(raw);
      if (modes.has("events") && raw.event !== "metadata") {
        parts.push({ event: "events", data: { event: raw.event === "end" ? "on_chain_end"
          : raw.event === "run" ? "on_chain_start" : "on_chain_stream", name: run.assistant_id,
          run_id: run.run_id, tags: [], metadata: {}, data: { chunk: raw.data }, parent_ids: [] } });
      }
      if (modes.has("debug") && raw.event === "updates") {
        const node = Object.keys(data)[0] ?? "unknown";
        parts.push({ event: "debug", data: { type: "task_result", timestamp: run.updated_at,
          payload: { name: node, result: data[node] } } });
      }
      if (raw.event === "end") parts.push(raw);
      for (let index = 0; index < parts.length; index++) {
        if (afterCursor(source, index, cursor)) yield { ...parts[index]!, id: `${source}.${index}` };
      }
    }
  }
}
