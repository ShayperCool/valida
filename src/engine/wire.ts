import { isBaseMessage } from "@langchain/core/messages";

const constructorTypes: Record<string, string> = {
  HumanMessage: "human", HumanMessageChunk: "human",
  AIMessage: "ai", AIMessageChunk: "ai",
  SystemMessage: "system", SystemMessageChunk: "system",
  ToolMessage: "tool", ToolMessageChunk: "tool",
  FunctionMessage: "function", FunctionMessageChunk: "function",
  ChatMessage: "chat", ChatMessageChunk: "chat",
};

/** Convert LangChain messages to the Agent Protocol wire shape without changing graph state. */
export function toWire(value: unknown): unknown {
  if (isBaseMessage(value)) {
    return {
      type: value.getType(),
      ...Object.fromEntries(Object.entries(value)
        .filter(([key]) => !key.startsWith("lc_"))
        .map(([key, item]) => [key, toWire(item)])),
    };
  }
  if (Array.isArray(value)) return value.map(toWire);
  if (value && typeof value === "object") {
    const record = value as Record<string, unknown>;
    const name = Array.isArray(record.id) ? record.id.at(-1) : undefined;
    if (record.lc === 1 && record.type === "constructor" && typeof name === "string" && constructorTypes[name]) {
      const kwargs = toWire(record.kwargs) as Record<string, unknown>;
      return { type: constructorTypes[name],
        ...Object.fromEntries(Object.entries(kwargs).filter(([key]) => !key.startsWith("lc_"))) };
    }
    return Object.fromEntries(Object.entries(record).map(([key, item]) => [key, toWire(item)]));
  }
  return value;
}
