import { BaseCallbackHandler } from "@langchain/core/callbacks/base";
import { toWire } from "./wire.js";

export interface V1TraceEvent {
  event: string;
  name: string;
  run_id: string;
  tags: string[];
  metadata: Record<string, unknown>;
  data: Record<string, unknown>;
  parent_ids: string[];
}

type Persist = (event: V1TraceEvent) => Promise<unknown>;
type TraceRun = { parent?: string; name: string; tags: string[]; metadata: Record<string, unknown> };

const record = (value: unknown): Record<string, unknown> =>
  value !== null && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};

function serializedName(value: unknown, fallback: string): string {
  const id = record(value).id;
  return Array.isArray(id) && typeof id.at(-1) === "string" ? id.at(-1) as string : fallback;
}

/** Captures LangChain callback ancestry and tags during the same v3 graph execution. */
export class V1TraceHandler extends BaseCallbackHandler {
  name = "ValidaV1TraceHandler";
  readonly lc_prefer_chat_model_stream_events = true;
  private readonly runs = new Map<string, TraceRun>();
  private readonly chatModels = new Set<string>();

  constructor(private readonly persist: Persist) {
    super({ raiseError: true });
  }

  override copy(): V1TraceHandler { return this; }

  private register(id: string, parent: string | undefined, name: string,
    tags?: string[], metadata?: Record<string, unknown>): void {
    this.runs.set(id, { parent, name, tags: tags ?? [], metadata: metadata ?? {} });
  }

  private parentIds(id: string): string[] {
    const ids: string[] = [];
    const seen = new Set([id]);
    let parent = this.runs.get(id)?.parent;
    while (parent && !seen.has(parent)) {
      ids.unshift(parent);
      seen.add(parent);
      parent = this.runs.get(parent)?.parent;
    }
    return ids;
  }

  private async emit(kind: string, id: string, data: Record<string, unknown>, tags?: string[]): Promise<void> {
    const run = this.runs.get(id);
    const actualTags = tags ?? run?.tags ?? [];
    if (actualTags.includes("langsmith:hidden")) return;
    await this.persist({ event: kind, name: run?.name ?? "unknown", run_id: id,
      tags: actualTags, metadata: run?.metadata ?? {},
      data: toWire(data) as Record<string, unknown>, parent_ids: this.parentIds(id) });
  }

  async handleChainStart(chain: unknown, inputs: unknown, runId: string, parentRunId?: string,
    tags?: string[], metadata?: Record<string, unknown>, _runType?: string, runName?: string): Promise<void> {
    this.register(runId, parentRunId, runName ?? serializedName(chain, "chain"), tags, metadata);
    await this.emit("on_chain_start", runId, { input: inputs }, tags);
  }

  async handleChainEnd(outputs: unknown, runId: string, _parentRunId?: string, tags?: string[]): Promise<void> {
    await this.emit("on_chain_end", runId, { output: outputs }, tags);
  }

  async handleChainError(error: Error, runId: string, _parentRunId?: string, tags?: string[]): Promise<void> {
    await this.emit("on_chain_error", runId, { error: error.message }, tags);
  }

  async handleChatModelStart(model: unknown, messages: unknown, runId: string, parentRunId?: string,
    _extra?: Record<string, unknown>, tags?: string[], metadata?: Record<string, unknown>, runName?: string): Promise<void> {
    this.chatModels.add(runId);
    this.register(runId, parentRunId, runName ?? serializedName(model, "chat_model"), tags, metadata);
    await this.emit("on_chat_model_start", runId, { input: { messages } }, tags);
  }

  async handleLLMStart(model: unknown, prompts: string[], runId: string, parentRunId?: string,
    _extra?: Record<string, unknown>, tags?: string[], metadata?: Record<string, unknown>, runName?: string): Promise<void> {
    this.register(runId, parentRunId, runName ?? serializedName(model, "llm"), tags, metadata);
    await this.emit("on_llm_start", runId, { input: { prompts } }, tags);
  }

  async handleChatModelStreamEvent(event: unknown, runId: string, _parentRunId?: string, tags?: string[]): Promise<void> {
    const item = record(event);
    if (item.event !== "content-block-delta") return;
    const delta = record(item.delta);
    const content = delta.type === "text-delta" ? delta.text ?? "" : [delta];
    await this.emit("on_chat_model_stream", runId, { chunk: { type: "AIMessageChunk", content } }, tags);
  }

  async handleLLMNewToken(token: string, _idx: unknown, runId: string, _parentRunId?: string,
    tags?: string[], fields?: { chunk?: unknown }): Promise<void> {
    await this.emit(this.chatModels.has(runId) ? "on_chat_model_stream" : "on_llm_stream", runId,
      { chunk: fields?.chunk ?? token }, tags);
  }

  async handleLLMEnd(output: unknown, runId: string, _parentRunId?: string, tags?: string[]): Promise<void> {
    const kind = this.chatModels.has(runId) ? "on_chat_model_end" : "on_llm_end";
    await this.emit(kind, runId, { output }, tags);
  }

  async handleLLMError(error: Error, runId: string, _parentRunId?: string, tags?: string[]): Promise<void> {
    await this.emit("on_llm_error", runId, { error: error.message }, tags);
  }

  async handleToolStart(tool: unknown, input: string, runId: string, parentRunId?: string,
    tags?: string[], metadata?: Record<string, unknown>, runName?: string): Promise<void> {
    this.register(runId, parentRunId, runName ?? serializedName(tool, "tool"), tags, metadata);
    await this.emit("on_tool_start", runId, { input }, tags);
  }

  async handleToolEvent(chunk: unknown, runId: string, _parentRunId?: string, tags?: string[]): Promise<void> {
    await this.emit("on_tool_stream", runId, { chunk }, tags);
  }

  async handleToolEnd(output: unknown, runId: string, _parentRunId?: string, tags?: string[]): Promise<void> {
    await this.emit("on_tool_end", runId, { output }, tags);
  }

  async handleToolError(error: Error, runId: string, _parentRunId?: string, tags?: string[]): Promise<void> {
    await this.emit("on_tool_error", runId, { error: error.message }, tags);
  }

  async handleRetrieverStart(retriever: unknown, query: string, runId: string, parentRunId?: string,
    tags?: string[], metadata?: Record<string, unknown>, name?: string): Promise<void> {
    this.register(runId, parentRunId, name ?? serializedName(retriever, "retriever"), tags, metadata);
    await this.emit("on_retriever_start", runId, { input: query }, tags);
  }

  async handleRetrieverEnd(documents: unknown, runId: string, _parentRunId?: string, tags?: string[]): Promise<void> {
    await this.emit("on_retriever_end", runId, { output: documents }, tags);
  }

  async handleRetrieverError(error: Error, runId: string, _parentRunId?: string, tags?: string[]): Promise<void> {
    await this.emit("on_retriever_error", runId, { error: error.message }, tags);
  }

  async handleCustomEvent(name: string, data: unknown, runId: string, tags?: string[],
    metadata?: Record<string, unknown>): Promise<void> {
    if (!this.runs.has(runId)) this.register(runId, undefined, name, tags, metadata);
    await this.emit("on_custom_event", runId, { name, data }, tags);
  }
}
