import { expect, test } from "bun:test";
import { Annotation, END, START, StateGraph, StateSchema } from "@langchain/langgraph";
import { z } from "zod";
import { approval, counter, echo } from "../../examples/graphs.ts";
import { describeGraph } from "./introspection.ts";

test("describes real echo, counter and approval nodes, edges and state keys", () => {
  const examples = [
    [echo, ["reply"], ["messages"]],
    [counter, ["add"], ["count", "increment"]],
    [approval, ["ask", "finish"], ["proposal", "approved", "result", "messages"]],
  ] as const;
  for (const [graph, nodes, keys] of examples) {
    const description = describeGraph(graph);
    expect(description.graph.nodes.map(node => node.id)).toEqual(["__start__", ...nodes, "__end__"]);
    expect(description.graph.edges.some(edge => edge.source === "__start__" && edge.target === nodes[0])).toBe(true);
    expect(Object.keys(description.schemas.state_schema.properties as object)).toEqual([...keys]);
    expect(Object.keys(description.schemas.input_schema.properties as object)).toEqual([...keys]);
    expect(Object.keys(description.subgraphs)).toEqual([]);
  }
  expect((describeGraph(echo).schemas.state_schema.properties as Record<string, { type?: string }>).messages?.type)
    .toBe("array");
});

test("describes a compiled child graph through public getSubgraphs", () => {
  const state = Annotation.Root({ count: Annotation<number> });
  const child = new StateGraph(state)
    .addNode("increment", value => ({ count: value.count + 1 }))
    .addEdge(START, "increment").addEdge("increment", END).compile();
  const parent = new StateGraph(state)
    .addNode("child", child).addEdge(START, "child").addEdge("child", END).compile();
  const described = describeGraph(parent);
  expect(Object.keys(described.subgraphs)).toEqual(["child"]);
  expect(described.subgraphs.child?.graph.nodes.map(node => node.id)).toEqual(["__start__", "increment", "__end__"]);
  expect(described.subgraphs.child?.graph.edges).toContainEqual({ source: "increment", target: "__end__", conditional: false });
});

test("uses StateSchema runtime JSON Schema when available", () => {
  const state = new StateSchema({ count: z.number(), label: z.string().optional() });
  const graph = new StateGraph(state)
    .addNode("increment", value => ({ count: value.count + 1 }))
    .addEdge(START, "increment").addEdge("increment", END).compile();
  const described = describeGraph(graph);
  expect((described.schemas.state_schema.properties as Record<string, { type: string }>).count?.type).toBe("number");
  expect(described.schemas.state_schema.required).toEqual(["count"]);
  expect(described.schemas.input_schema.required).toBeUndefined();
});
