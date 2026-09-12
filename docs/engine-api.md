# Graph runtime API

`createRuntime` opens a Drizzle-backed store and creates its tables. SQLite runs in one process without Redis. PostgreSQL plus BullMQ lets HTTP instances enqueue runs and workers process them on other machines.

```ts
import { createRuntime } from "../src/engine/index.ts";

const runtime = await createRuntime({ db: { dialect: "sqlite", url: "./data/valida.db" } });
runtime.registerGraph({
  id: "approval",
  entrypoint: "calculate",
  nodes: {
    calculate: state => ({ result: Number(state.input) * 2 }),
    approve: (_state, context) => ({ approved: context.interrupt("Approve result?") }),
  },
  edges: { calculate: "approve", approve: "__end__" },
});

const thread = await runtime.createThread();
const run = await runtime.startRun({ threadId: thread.id, graphId: "approval", input: { input: 4 } });
await runtime.waitRun(run.id); // interrupted
const resumed = await runtime.resumeRun({ threadId: thread.id, resume: true });
await runtime.waitRun(resumed.id); // success
console.log((await runtime.getState(thread.id))?.values); // { input: 4, result: 8, approved: true }
```

The custom graph contract accepts a state update from each node. `context.interrupt(value)` saves the current node and its state; the resumed run retries that node and returns the supplied resume value. A node can instead return `{ update, goto }` for conditional routing. `reducers` can merge individual state keys, such as appending messages. Without a reducer, each update replaces that key. `edges` arrays run in order; this custom engine does not execute nodes in parallel.

Compiled `StateGraph` instances from `@langchain/langgraph` are also supported. The runtime attaches a durable `DrizzleCheckpointer`, including pending writes needed after process restart:

```ts
import { StateGraph, Annotation, START, END, interrupt } from "@langchain/langgraph";

const schema = Annotation.Root({ approved: Annotation<boolean> });
const graph = new StateGraph(schema)
  .addNode("ask", () => ({ approved: interrupt("Approve?") as boolean }))
  .addEdge(START, "ask").addEdge("ask", END).compile();
runtime.registerGraph({ id: "approval", graph });
```

For distributed deployment, use the same PostgreSQL database and Redis URL on every instance:

```ts
const runtime = await createRuntime({
  db: { dialect: "postgres", url: process.env.DATABASE_URL! },
  queue: { redisUrl: process.env.REDIS_URL!, concurrency: 4 },
});
runtime.registerGraph({ id: "approval", graph });
runtime.startWorker(); // call only in worker processes
```

When `queue` is set, `startRun` and `resumeRun` enqueue durable BullMQ jobs. API processes can omit `startWorker`. Set `inline: true` only when the current instance should also process runs. The queue job ID is the run ID, and database run claiming prevents duplicate workers from executing the same pending run.

`runtime.store` exposes assistant, thread, run, checkpoint, and event repository methods. `runtime.stream(runId, { after })` replays stored events and follows a live run. Event names include `metadata`, `run`, `updates`, `values`, `error`, and `end`. `runtime.getHistory(threadId)` returns persisted snapshots. `runtime.updateState(threadId, values, asNode?)` updates both the API snapshot and the native LangGraph checkpoint for compiled graphs.

Compiled graph events, API snapshots, and run output convert LangChain message instances to Agent Protocol objects such as `{ type: "ai", content: "Hello", id: "..." }`. The native LangGraph checkpoint retains its typed message objects for subsequent graph execution.

Drizzle schemas live in `src/db/schema.sqlite.ts` and `src/db/schema.pg.ts`. Startup `Store.migrate()` creates the baseline tables for both dialects. Generate versioned SQL migrations with `bunx drizzle-kit generate --config drizzle.sqlite.config.ts` and `bunx drizzle-kit generate --config drizzle.pg.config.ts` after schema changes. The runtime needs `drizzle-orm`, `postgres`, `bullmq`, `ioredis`, `@langchain/core`, `@langchain/langgraph`, and `@langchain/langgraph-checkpoint` as direct dependencies.
