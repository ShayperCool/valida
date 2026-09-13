# Valida

Valida is a TypeScript server for running LangGraph graphs through the Agent Protocol. It targets Bun 1.4.1 and uses Drizzle for SQLite and PostgreSQL storage. The official [LangGraph SDK](https://github.com/langchain-ai/langgraph) and [Agent Chat UI](https://github.com/langchain-ai/agent-chat-ui) connect to its HTTP API.

This is a fresh TypeScript implementation inspired by [Aegra](https://github.com/aegra/aegra). It does not bundle a chat frontend. Graphs in this repository make no LLM calls, so tests run without API keys.

## Run locally

Install [Bun 1.4.1](https://bun.sh/docs/installation), then:

```bash
bun install
cp .env.example .env
bun run serve
```

The server listens at `http://127.0.0.1:2026`. It uses `data/valida.db` by default and runs jobs in the API process. `/health` checks startup, and `/assistants/search` lists the deterministic `echo`, `counter`, and `approval` graphs from [valida.json](./valida.json).

The graph setting accepts `./path/to/file.ts:exportName`. Export a compiled `StateGraph`, a graph builder that accepts a checkpointer, or the small node/edge definition described in [docs/engine-api.md](./docs/engine-api.md). Valida attaches a Drizzle-backed LangGraph checkpointer to compiled graphs, including pending writes needed to resume an interrupt after restart.

```ts
import { Client } from "@langchain/langgraph-sdk";

const client = new Client({ apiUrl: "http://127.0.0.1:2026" });
const thread = await client.threads.create();
const result = await client.runs.wait(thread.thread_id, "counter", {
  input: { count: 3, increment: 4 },
});
console.log(result); // { count: 7, increment: 4 }
```

`approval` pauses with an Agent Inbox approval request. Resume it with `command: { resume: { decisions: [{ type: "approve" }] } }` or click Approve/Reject in Agent Chat UI. The `echo` graph returns a deterministic reply to the last human message.

The official `RemoteGraph` client works with the same API. Omit `thread_id` for a stateless run; pass it for persistent state, checkpoints, and history. A `RemoteGraph` can also be a node in another `StateGraph`.

```ts
import { RemoteGraph } from "@langchain/langgraph/remote";

const remote = new RemoteGraph({ graphId: "counter", client });
await remote.invoke({ count: 1, increment: 2 }); // stateless: count 3
const remoteThread = await client.threads.create();
const config = { configurable: { thread_id: remoteThread.thread_id } };
await remote.invoke({ count: 3, increment: 2 }, config); // stateful: count 5
const checkpoint = await remote.getState(config);
```

## Connect the official Agent Chat UI

Run the [official UI](https://github.com/langchain-ai/agent-chat-ui) in another directory:

```bash
git clone https://github.com/langchain-ai/agent-chat-ui.git
cd agent-chat-ui
corepack pnpm install
NEXT_PUBLIC_API_URL=http://127.0.0.1:2026 NEXT_PUBLIC_ASSISTANT_ID=echo corepack pnpm dev
```

Open `http://localhost:3000`. Select `approval` with `?assistantId=approval` to try HITL. We verified chat, follow-up turns, history after reload, regeneration, and Approve/Reject against this UI. For a different host or port, set `NEXT_PUBLIC_API_URL` to Valida's reachable API URL and configure `http.cors.allow_origins` in `valida.json`.

For CopilotKit or another AG-UI client, point its official `HttpAgent` at `POST /ag-ui/:assistantId`. This backend endpoint streams text, graph tool calls/results, state snapshots, and HITL outcomes. It uses the same auth middleware as the Agent Protocol routes. See [AG-UI endpoint](./src/api/ag_ui.md) for the supported event set and current limits. It does not add a UI to this repository.

## PostgreSQL, Redis, and workers

```bash
docker compose up --build
```

This starts PostgreSQL, Redis, and an API instance that also runs BullMQ jobs. Add separate worker processes with `docker compose --profile extra-workers up --build --scale worker=2`. API instances share the database and queue; put them behind a load balancer when scaling HTTP traffic. Runs and SSE events are stored in PostgreSQL, so a client can create a run through one API instance and read its result through another.

Workers renew a database lease throughout execution and poll PostgreSQL for runnable work if Redis is unavailable. Cancellation from another instance aborts a running graph after its next lease check; custom nodes receive `context.signal` so they can stop long work promptly. The default run deadline is one hour. Set `execution.timeout_ms` in `valida.json` or `RUN_TIMEOUT_MS` in the environment; `0` disables the deadline. The `execution.concurrency`, `execution.lease_ms`, and `execution.recovery_poll_ms` settings tune worker throughput and recovery. A node that ignores abort can still perform external side effects after cancellation, although its late result cannot replace the terminal run state.

For a database-free service stack, use `docker compose -f compose.standalone.yml up --build`. That mode uses a persistent SQLite volume and requires no Redis. To run a separate worker directly, set `EXECUTION_MODE=distributed`, `DATABASE_URL=postgres://...`, and `REDIS_URL=redis://...`, then run `bun run worker`.

Stateless runs retain their internal ephemeral thread for 24 hours so clients can join and replay events. A background sweep then removes completed ephemeral threads and their checkpoints/events. `client.threads.prune(ids)` explicitly deletes finished threads; active runs are skipped. With `{ strategy: "keep_latest" }`, it keeps the thread's most recent checkpoint in each namespace and the pending writes needed for HITL resume.

Thread TTL is opt-in. Without `checkpointer.ttl` or `VALIDA_THREAD_TTL`, ordinary threads have no automatic expiry. An explicit `client.threads.create({ ttl: 30 })` sets a 30-minute deadline; the sweeper removes the thread after that deadline, normally within another five minutes. `client.threads.update(id, { ttl: 60 })` resets the deadline to 60 minutes from the update. Use `{ ttl: 60, strategy: "delete" }` or a raw API request with `strategy: "keep_latest"`; the latter compacts checkpoint history and re-arms the deadline. To set a default for newly created threads, add `"checkpointer": { "ttl": { "default_ttl": 43200, "strategy": "delete", "sweep_interval_minutes": 5, "sweep_limit": 1000 } }` to `valida.json`. An empty `checkpointer.ttl` block also enables the 43200-minute default. `VALIDA_THREAD_TTL` accepts a minute count or the same JSON object and overrides the config block. The sweeper leaves active runs alone until they finish.

Both database schemas live in `src/db/schema.sqlite.ts` and `src/db/schema.pg.ts`. Versioned Drizzle migrations live in `drizzle/sqlite` and `drizzle/pg`. Use `bun run db:generate:sqlite` or `bun run db:generate:pg` after a schema change, followed by the matching `db:migrate:*` command. Startup creates missing tables for local development; the migrations use idempotent table and index creation so they can also be applied to a database initialized at startup.

To try semantic store search without an LLM, start with `VALIDA_CONFIG=examples/valida.semantic.json bun run serve`. That config loads the deterministic embedding function in `examples/embeddings.ts`. A production embedding function can use the same TypeScript module interface. SQLite ranks matching rows in application memory; PostgreSQL uses pgvector and a dimension-specific HNSW index. The Compose PostgreSQL image includes the `vector` extension; other PostgreSQL deployments must install it before enabling the index. Items written before enabling an index need a `put` to create embeddings. PostgreSQL automatically imports existing JSON embeddings from earlier Valida versions without calling the embedding function again.

For tracing, set `OTEL_EXPORTER_OTLP_ENDPOINT` or `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`. HTTP requests and graph runs export spans, including a shared trace across BullMQ workers. `OTEL_TARGETS=LANGFUSE,PHOENIX,GENERIC` sends the same spans to multiple backends. Langfuse uses `LANGFUSE_BASE_URL`, `LANGFUSE_PUBLIC_KEY`, and `LANGFUSE_SECRET_KEY`; Phoenix uses `PHOENIX_COLLECTOR_ENDPOINT` and optional `PHOENIX_API_KEY`. Set `OTEL_CONSOLE_EXPORT=true` for local stdout traces.

## Authentication and middleware

Valida accepts TypeScript modules in `valida.json`:

```json
{
  "graphs": { "echo": "./examples/graphs.ts:echo" },
  "auth": { "path": "./examples/auth.ts:auth" },
  "http": {
    "middleware": ["./examples/middleware.ts:requestTiming"],
    "middleware_order": "auth_first",
    "cors": { "allow_origins": ["http://localhost:3000"] }
  }
}
```

An auth provider exports `authenticate(request)` and an optional `authorize({ user, resource, action, permissions, path, method, params, query }, value)`. It can verify a JWT, API key, or another credential, deny a resource operation, replace a write payload, or return a filter for reads/searches. Filters apply to threads, assistants, cron jobs, and store items. New threads record an owner and reads/searches also filter by that identity. A custom Hono app can be mounted with `http.app`; set `http.enable_custom_route_auth` if its routes need the same authentication. The example auth module uses `VALIDA_DEMO_TOKEN` and is disabled by default.

## Current compatibility

Valida implements assistants and version snapshots, graph schemas/topology, threads, runs, checkpoint state/history, SSE replay, HITL `resume`/`update`/`goto`, namespaced JSON and optional semantic search, and cron scheduling. Compiled graphs expose native v2 token content-block, tool, and subgraph events. The official LangGraph SDK, Agent Chat UI, RemoteGraph, and AG-UI `HttpAgent` are covered by deterministic integration tests. OpenTelemetry tracing can fan out to OTLP, Langfuse, and Phoenix. The store returns 501 for semantic queries until an embedding index is configured. See [src/api/README.md](./src/api/README.md) and [src/extensions/README.md](./src/extensions/README.md) for the precise endpoint behavior.

Run `bun run typecheck` and `bun test` to verify the protocol and graph runtime. With a server running, `bun run smoke` exercises the deployed API through the SDK and RemoteGraph. Set `VALIDA_API_URL` and `VALIDA_API_URL_2` to different API instances to verify cross-instance state and execution. Tests use SQLite and deterministic graphs; the distributed path has also been exercised with PostgreSQL, Redis, separate API and worker processes.

## Releases

The manual GitHub Actions release workflow tests Bun 1.4.1, builds a versioned source archive with `bun.lock`, verifies its SHA-256 checksum, and runs the SDK smoke test from an unpacked archive. It then publishes a Linux amd64/arm64 container image to GHCR and creates a GitHub Release tagged from `package.json` (for example, `v0.1.0`). The same image runs the API by default or a separate worker with `bun src/worker.ts`.

To verify a release archive locally, run `bash scripts/build-release.sh` and `bash scripts/release-smoke.sh`. Releases do not publish to npm; install the archive with Bun or use the container image.
