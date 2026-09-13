/** The JSON records exchanged by the LangGraph SDK use snake_case keys. */
export type JsonRecord = Record<string, unknown>;

export interface Assistant extends JsonRecord {
  assistant_id: string;
  graph_id: string;
  name: string;
  description?: string | null;
  config: JsonRecord;
  context?: JsonRecord;
  metadata: JsonRecord;
  version?: number;
  created_at: string;
  updated_at: string;
}

export interface Thread extends JsonRecord {
  thread_id: string;
  status: "idle" | "busy" | "interrupted" | "error";
  metadata: JsonRecord;
  created_at: string;
  updated_at: string;
}

export interface Checkpoint extends JsonRecord {
  thread_id?: string;
  checkpoint_id?: string;
  checkpoint_ns?: string;
}

export interface ThreadState extends JsonRecord {
  values: JsonRecord;
  next: string[];
  tasks: JsonRecord[];
  interrupts: JsonRecord[];
  metadata: JsonRecord;
  checkpoint: Checkpoint | null;
  parent_checkpoint: Checkpoint | null;
  created_at: string | null;
}

export interface Run extends JsonRecord {
  run_id: string;
  thread_id: string;
  assistant_id: string;
  status: "pending" | "running" | "success" | "error" | "interrupted" | "timeout";
  created_at: string;
  updated_at: string;
}

export interface StreamEvent {
  event: string;
  data: unknown;
  id?: string;
}

export interface ApiRequestContext {
  request: Request;
  /** Set by a custom Hono authentication middleware through `c.set("principal", value)`. */
  principal?: unknown;
}

export interface PlatformAdapter {
  assistants: {
    create(payload: JsonRecord, context: ApiRequestContext): Promise<Assistant>;
    search(query: JsonRecord, context: ApiRequestContext): Promise<Assistant[]>;
    get(id: string, context: ApiRequestContext): Promise<Assistant | null>;
    update(id: string, payload: JsonRecord, context: ApiRequestContext): Promise<Assistant | null>;
    delete(id: string, context: ApiRequestContext): Promise<boolean>;
    count?(query: JsonRecord, context: ApiRequestContext): Promise<number>;
    versions?(id: string, query: JsonRecord, context: ApiRequestContext): Promise<Assistant[]>;
    setLatest?(id: string, version: number, context: ApiRequestContext): Promise<Assistant | null>;
    graph?(id: string, query: JsonRecord, context: ApiRequestContext): Promise<JsonRecord | null>;
    schemas?(id: string, context: ApiRequestContext): Promise<JsonRecord | null>;
    subgraphs?(id: string, query: JsonRecord, context: ApiRequestContext): Promise<JsonRecord | null>;
  };
  threads: {
    create(payload: JsonRecord, context: ApiRequestContext): Promise<Thread>;
    search(query: JsonRecord, context: ApiRequestContext): Promise<Thread[]>;
    get(id: string, context: ApiRequestContext): Promise<Thread | null>;
    update(id: string, payload: JsonRecord, context: ApiRequestContext): Promise<Thread | null>;
    delete(id: string, context: ApiRequestContext): Promise<boolean>;
    getState(id: string, checkpoint: Checkpoint | string | null, context: ApiRequestContext): Promise<ThreadState | null>;
    updateState(id: string, payload: JsonRecord, context: ApiRequestContext): Promise<JsonRecord | null>;
    history(id: string, query: JsonRecord, context: ApiRequestContext): Promise<ThreadState[] | null>;
    copy?(id: string, context: ApiRequestContext): Promise<Thread | null>;
    count?(query: JsonRecord, context: ApiRequestContext): Promise<number>;
    prune?(payload: JsonRecord, context: ApiRequestContext): Promise<JsonRecord>;
  };
  runs: {
    /** A null thread ID requests a stateless run; implementations may create an ephemeral thread. */
    create(threadId: string | null, payload: JsonRecord, context: ApiRequestContext): Promise<Run>;
    get(threadId: string | null, runId: string, context: ApiRequestContext): Promise<Run | null>;
    list(threadId: string, query: JsonRecord, context: ApiRequestContext): Promise<Run[]>;
    /** Resolve to the final graph values. For HITL interruption, return current values. */
    join(threadId: string | null, runId: string, context: ApiRequestContext): Promise<unknown>;
    /** Replay after lastEventId and then follow a live run. Emit LangGraph SSE event names. */
    events(threadId: string | null, runId: string, lastEventId: string | null, context: ApiRequestContext): AsyncIterable<StreamEvent>;
    cancel(threadId: string, runId: string, action: "interrupt" | "rollback", context: ApiRequestContext): Promise<boolean>;
    update?(threadId: string, runId: string, payload: JsonRecord, context: ApiRequestContext): Promise<Run | null>;
    delete?(threadId: string, runId: string, context: ApiRequestContext): Promise<boolean>;
  };
  /** Optional Agent Protocol v2 support for the latest SDK thread stream. */
  v2?: {
    command(threadId: string, body: JsonRecord, context: ApiRequestContext): Promise<JsonRecord>;
    events(threadId: string, body: JsonRecord, context: ApiRequestContext): AsyncIterable<StreamEvent>;
  };
  /** Optional projection of persisted run events into legacy stream_mode SSE. */
  v1?: {
    events(threadId: string | null, run: Run, body: JsonRecord, context: ApiRequestContext,
      lastEventId?: string | null): AsyncIterable<StreamEvent>;
  };
  store?: {
    put(payload: JsonRecord, context: ApiRequestContext): Promise<void>;
    get(namespace: string[], key: string, context: ApiRequestContext): Promise<JsonRecord | null>;
    delete(namespace: string[], key: string, context: ApiRequestContext): Promise<void>;
    search(payload: JsonRecord, context: ApiRequestContext): Promise<JsonRecord>;
    namespaces(payload: JsonRecord, context: ApiRequestContext): Promise<JsonRecord>;
  };
  crons?: {
    create(threadId: string | null, payload: JsonRecord, context: ApiRequestContext): Promise<JsonRecord>;
    update(id: string, payload: JsonRecord, context: ApiRequestContext): Promise<JsonRecord | null>;
    delete(id: string, context: ApiRequestContext): Promise<boolean>;
    search(payload: JsonRecord, context: ApiRequestContext): Promise<JsonRecord[]>;
    count(payload: JsonRecord, context: ApiRequestContext): Promise<number>;
  };
  health?(context: ApiRequestContext): Promise<JsonRecord>;
  info?(context: ApiRequestContext): Promise<JsonRecord>;
}

export class ApiError extends Error {
  constructor(public readonly status: number, message: string) {
    super(message);
    this.name = "ApiError";
  }
}
