# AG-UI endpoint

Mount `createAgUiApi(adapter, runtime)` behind the same authentication and custom middleware as the Agent Protocol API. An official `@ag-ui/client` `HttpAgent` can use `POST /ag-ui/:assistantId`, where `assistantId` is an existing Valida assistant or graph ID.

The endpoint accepts `RunAgentInput` from `@ag-ui/core`. It uses `threadId` for the Valida thread, appends only messages newer than the checkpoint, passes the supplied shared `state` into the graph, and resumes one pending LangGraph interrupt from `resume[0]`. `forwardedProps.config` and `forwardedProps.metadata` are passed to the run. Responses use the official `@ag-ui/encoder` for SSE or negotiated AG-UI protobuf.

It emits `RUN_STARTED`, streamed assistant text, graph tool calls and results, `STATE_SNAPSHOT`, and terminal `RUN_FINISHED` or `RUN_ERROR`. An interrupted run ends with `RUN_FINISHED.outcome.type = "interrupt"` and a stable interrupt ID; reply with `resume: [{ interruptId, status: "resolved", payload }]` on the same thread.

Current limits: one pending interrupt at a time; `cancelled` resume entries, executable client-provided tools, multimodal output, reasoning events, state deltas, and persistent `connectAgent` sessions are not implemented. The Agent Protocol and LangGraph SDK endpoints remain available for their native capabilities.
