import { Annotation, END, START, StateGraph } from "@langchain/langgraph";

const BenchmarkState = Annotation.Root({
  seed: Annotation<number>,
  increment: Annotation<number>,
  intermediate: Annotation<number>,
  result: Annotation<number>,
});

/** Two deterministic 200 ms steps; { seed: 7, increment: 5 } produces result 24. */
export const benchmark = new StateGraph(BenchmarkState)
  .addNode("add", async state => {
    await Bun.sleep(200);
    return { intermediate: state.seed + state.increment };
  })
  .addNode("double", async state => {
    await Bun.sleep(200);
    return { result: state.intermediate * 2 };
  })
  .addEdge(START, "add")
  .addEdge("add", "double")
  .addEdge("double", END)
  .compile();
