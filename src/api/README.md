# Agent Protocol HTTP adapter

`createApi(adapter)` mounts LangGraph SDK routes on Hono. The adapter in
`types.ts` owns data access, execution, and per-user authorization. Middleware
can set `principal` on the Hono context; methods also receive the raw request.

The legacy run endpoints emit SSE with `event`, JSON `data`, and replayable `id`.
`POST /threads/:id/runs/wait` and `GET /threads/:id/runs/:runId/join` return
the graph's final values. HITL resumption sends `command.resume` through the
same run creation method as a fresh input.

The built-in v2 bridge supports the current SDK's `threads.stream()` over
HTTP SSE. It maps `run.start` and `input.respond` to adapter runs and projects
persisted `values`, `updates`, `input.requested`, and lifecycle events. This
bridge cannot synthesize token-level content-block messages, tool events, or
subgraph lifecycle events from legacy run events. Implement `adapter.v2` to
serve those from a runtime that produces native v3 events.

Upstream references: [Aegra routes](https://github.com/aegra/aegra/tree/main/libs/aegra-api/src/aegra_api/api),
[LangGraph JS SDK](https://github.com/langchain-ai/langgraphjs/tree/main/libs/sdk/src/client).
