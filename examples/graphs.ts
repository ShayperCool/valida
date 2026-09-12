import { AIMessage } from "@langchain/core/messages";
import { Annotation, END, interrupt, MessagesAnnotation, START, StateGraph } from "@langchain/langgraph";

/** Deterministic chat graph. Its reply depends only on the last user message. */
export const echo = new StateGraph(MessagesAnnotation)
  .addNode("reply", state => {
    const last = [...state.messages].reverse().find(message => message.getType() === "human");
    const content = typeof last?.content === "string" ? last.content : JSON.stringify(last?.content ?? "");
    return { messages: [new AIMessage(`Echo: ${content}`)] };
  })
  .addEdge(START, "reply")
  .addEdge("reply", END)
  .compile();

const CounterState = Annotation.Root({
  count: Annotation<number>,
  increment: Annotation<number>,
});
export const counter = new StateGraph(CounterState)
  .addNode("add", state => ({ count: (state.count ?? 0) + (state.increment ?? 1) }))
  .addEdge(START, "add")
  .addEdge("add", END)
  .compile();

const ApprovalState = Annotation.Root({
  proposal: Annotation<string>,
  approved: Annotation<boolean>,
  result: Annotation<string>,
});
export const approval = new StateGraph(ApprovalState)
  .addNode("ask", state => {
    const approved = interrupt({ kind: "approval", proposal: state.proposal });
    return { approved: Boolean(approved) };
  })
  .addNode("finish", state => ({ result: state.approved ? `Approved: ${state.proposal}` : `Rejected: ${state.proposal}` }))
  .addEdge(START, "ask")
  .addEdge("ask", "finish")
  .addEdge("finish", END)
  .compile();
