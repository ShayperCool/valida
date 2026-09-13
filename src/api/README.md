# Agent Protocol HTTP adapter

`createApi(adapter)` mounts LangGraph SDK routes on Hono. The adapter in
`types.ts` owns data access, execution, and per-user authorization. Middleware
can set `principal` on the Hono context; methods also receive the raw request.

The legacy run endpoints emit SSE with `event`, JSON `data`, and replayable `id`.
`POST /threads/:id/runs/wait` and `GET /threads/:id/runs/:runId/join` return
the graph's final values. HITL resumption sends `command.resume`, `command.update`, and `command.goto` through the
same run creation method as a fresh input.

`NativeV2Bridge` supports the current SDK's `threads.stream()` over HTTP SSE.
It maps `run.start` and `input.respond` to adapter runs, serves native v3
`ProtocolEvent` envelopes from compiled graphs, and preserves token content
blocks, tool events, subgraph namespaces, and thread-wide replay cursors.
Custom graphs without native events use the legacy projection. `main.ts`
mounts this bridge as `adapter.v2`.

Upstream references: [Aegra routes](https://github.com/aegra/aegra/tree/main/libs/aegra-api/src/aegra_api/api),
[LangGraph JS SDK](https://github.com/langchain-ai/langgraphjs/tree/main/libs/sdk/src/client).
