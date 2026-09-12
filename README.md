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

## Connect the official Agent Chat UI

Run the [official UI](https://github.com/langchain-ai/agent-chat-ui) in another directory:

```bash
git clone https://github.com/langchain-ai/agent-chat-ui.git
cd agent-chat-ui
corepack pnpm install
NEXT_PUBLIC_API_URL=http://127.0.0.1:2026 NEXT_PUBLIC_ASSISTANT_ID=echo corepack pnpm dev
```

Open `http://localhost:3000`. Select `approval` with `?assistantId=approval` to try HITL. We verified chat, follow-up turns, history after reload, regeneration, and Approve/Reject against this UI. For a different host or port, set `NEXT_PUBLIC_API_URL` to Valida's reachable API URL and configure `http.cors.allow_origins` in `valida.json`.

## PostgreSQL, Redis, and workers

```bash
docker compose up --build
```

This starts PostgreSQL, Redis, and an API instance that also runs BullMQ jobs. Add separate worker processes with `docker compose --profile extra-workers up --build --scale worker=2`. API instances share the database and queue; put them behind a load balancer when scaling HTTP traffic. Runs and SSE events are stored in PostgreSQL, so a client can create a run through one API instance and read its result through another.

For a database-free service stack, use `docker compose -f compose.standalone.yml up --build`. That mode uses a persistent SQLite volume and requires no Redis. To run a separate worker directly, set `EXECUTION_MODE=distributed`, `DATABASE_URL=postgres://...`, and `REDIS_URL=redis://...`, then run `bun run worker`.

Both database schemas live in `src/db/schema.sqlite.ts` and `src/db/schema.pg.ts`. Generated migrations live in `drizzle/sqlite` and `drizzle/pg`. Use `bun run db:generate:sqlite` or `bun run db:generate:pg` after a schema change, followed by the matching `db:migrate:*` command. Startup creates missing tables for local development; the initial Drizzle migration can also be applied to a database already initialized that way.

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

An auth provider exports `authenticate(request)` and an optional `authorize({ user, resource, action, permissions }, value)`. It can verify a JWT, API key, or another credential and deny a resource operation. When authentication is enabled, new threads record an owner and thread reads/searches filter by that identity. A custom Hono app can be mounted with `http.app`; set `http.enable_custom_route_auth` if its routes need the same authentication. The example auth module uses `VALIDA_DEMO_TOKEN` and is disabled by default.

## Current compatibility

Valida implements assistants, threads, runs, checkpoint state/history, SSE replay, HITL resume, exact-key namespaced JSON store, and cron scheduling. The HTTP v2 bridge supports state/lifecycle streams and HITL in the current SDK. Native token content-block streams, tool/subgraph streaming, semantic vector search, assistant versioning, and OpenTelemetry integrations from upstream Aegra are not implemented yet. The store returns 501 for semantic queries until a vector index is configured. See [src/api/README.md](./src/api/README.md) and [src/extensions/README.md](./src/extensions/README.md) for the precise endpoint behavior.

Run `bun run typecheck` and `bun test` to verify the protocol and graph runtime. Tests use SQLite and deterministic graphs; the distributed path has also been exercised with PostgreSQL, Redis, separate API and worker processes.
