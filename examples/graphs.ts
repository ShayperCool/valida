import { AIMessage, type BaseMessage } from "@langchain/core/messages";
import { Annotation, END, interrupt, MessagesAnnotation, messagesStateReducer, START, StateGraph } from "@langchain/langgraph";

function lastHumanText(messages: BaseMessage[]): string {
  const last = [...messages].reverse().find(message => message.getType() === "human");
  return typeof last?.content === "string" ? last.content
    : Array.isArray(last?.content)
      ? last.content.map(block => typeof block === "string" ? block
        : block && typeof block === "object" && "text" in block ? String(block.text) : "").join("")
      : "";
}

/** Deterministic chat graph. Its reply depends only on the last user message. */
export const echo = new StateGraph(MessagesAnnotation)
  .addNode("reply", state => {
    const content = lastHumanText(state.messages);
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
  messages: Annotation<BaseMessage[]>({ reducer: messagesStateReducer, default: () => [] }),
});
export const approval = new StateGraph(ApprovalState)
  .addNode("ask", state => {
    const proposal = state.proposal || lastHumanText(state.messages);
    const decision = interrupt({
      action_requests: [{
        name: "approve_proposal",
        args: { proposal },
        description: `Approve proposal: ${proposal}`,
      }],
      review_configs: [{ action_name: "approve_proposal", allowed_decisions: ["approve", "reject"] }],
    });
    const approval = typeof decision === "boolean" ? decision
      : decision && typeof decision === "object" && "decisions" in decision
        ? Array.isArray(decision.decisions) && decision.decisions[0]?.type === "approve"
        : false;
    return { proposal, approved: approval };
  })
  .addNode("finish", state => {
    const result = state.approved ? `Approved: ${state.proposal}` : `Rejected: ${state.proposal}`;
    return { result, messages: [new AIMessage(result)] };
  })
  .addEdge(START, "ask")
  .addEdge("ask", "finish")
  .addEdge("finish", END)
  .compile();
