# AG-UI endpoint

Mount `createAgUiApi(adapter, runtime)` behind the same authentication and custom middleware as the Agent Protocol API. An official `@ag-ui/client` `HttpAgent` can use `POST /ag-ui/:assistantId`, where `assistantId` is an existing Valida assistant or graph ID. The endpoint accepts `RunAgentInput` from `@ag-ui/core` and responds with the official `@ag-ui/encoder` SSE stream or negotiated AG-UI protobuf.

`threadId` selects the durable Valida thread. On each run, Valida appends only messages newer than the checkpoint and passes non-message shared `state` fields into the graph. User content parts, including image, audio, video, document, and binary sources, are preserved in LangGraph messages. `tools` and `context` are available to nodes as `config.configurable.ag_ui_tools` and `config.configurable.ag_ui_context`; `forwardedProps.config` and `forwardedProps.metadata` are also forwarded. The graph can emit AG-UI tool calls, then a frontend can execute its own tool and send a `role: "tool"` message on the next run. Valida does not execute frontend tools on the server.

The stream emits `RUN_STARTED`, assistant text, tool calls and results, reasoning events for LangGraph reasoning blocks, `STATE_SNAPSHOT` followed by JSON Patch `STATE_DELTA` events as graph state changes, and terminal `RUN_FINISHED` or `RUN_ERROR`. Interrupted runs send final `STATE_SNAPSHOT` and `MESSAGES_SNAPSHOT` events before `RUN_FINISHED.outcome.type = "interrupt"`, including stable IDs for every pending interrupt. Resume on the same thread with one entry per pending ID:

```ts
await agent.runAgent({ resume: [
  { interruptId: first.id, status: "resolved", payload: decision },
  { interruptId: second.id, status: "cancelled" },
] });
```

A single resolved interrupt passes its payload to LangGraph `interrupt()`. A single cancelled interrupt passes `{ __agui_cancelled__: true, interrupt_id }`. With parallel interrupts, each resumed LangGraph node receives `{ __agui_resume_map__: { [interruptId]: { status, payload? } } }` and can select its own ID. Missing, unknown, or duplicate interrupt IDs are rejected before starting a new run.

The installed official `HttpAgent` only implements the POST run transport; its `connectAgent()` path throws `AGUIConnectNotImplementedError`, so a persistent attach or reconnect session cannot be enabled by this endpoint alone. AG-UI's current assistant message schema accepts text content, so multimodal assistant output is not representable as a standard `HttpAgent` message; use the native LangGraph SDK endpoints when a graph needs the original output blocks. See the [AG-UI interrupt protocol](https://docs.ag-ui.com/concepts/interrupts), [frontend tool contract](https://docs.ag-ui.com/concepts/tools), and [multimodal input contract](https://docs.ag-ui.com/sdk/js/core/multimodal-inputs).
