import { getJsonSchemaFromSchema } from "@langchain/langgraph";
import type { GraphDefinition } from "./index.js";

type JsonSchema = Record<string, unknown>;
export interface GraphTopology {
  nodes: Array<Record<string, unknown>>;
  edges: Array<Record<string, unknown>>;
}
export interface GraphSchemas {
  input_schema: JsonSchema;
  output_schema: JsonSchema;
  state_schema: JsonSchema;
  config_schema: JsonSchema;
}
export interface GraphDescription {
  graph: GraphTopology;
  schemas: GraphSchemas;
  subgraphs: Record<string, { graph: GraphTopology; schemas: GraphSchemas }>;
}

type GraphLike = {
  getGraph?: () => { toJSON?: () => unknown; nodes?: unknown; edges?: unknown };
  getInputSchema?: () => unknown;
  getOutputSchema?: () => unknown;
  getStateSchema?: () => unknown;
  getConfigSchema?: () => unknown;
  getSubgraphs?: (namespace?: string, recurse?: boolean) => Iterable<[string, GraphLike]>;
  builder?: Record<string, unknown>;
};

const isObject = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === "object" && !Array.isArray(value);
const emptySchema = (): JsonSchema => ({ type: "object", properties: {}, additionalProperties: true });
const isJsonSchema = (value: unknown): value is JsonSchema =>
  isObject(value) && (typeof value.type === "string" || isObject(value.properties) || typeof value.$schema === "string");

function schemaFrom(value: unknown): JsonSchema | null {
  if (!value) return null;
  if (isObject(value) && typeof value.getJsonSchema === "function") {
    const schema = value.getJsonSchema();
    if (isJsonSchema(schema)) return schema;
  }
  if (isObject(value) && typeof value.getInputJsonSchema === "function") {
    const schema = value.getInputJsonSchema();
    if (isJsonSchema(schema)) return schema;
  }
  const standard = getJsonSchemaFromSchema(value);
  if (isJsonSchema(standard)) return standard;
  if (isJsonSchema(value)) return value;
  return null;
}

function inputSchemaFrom(value: unknown): JsonSchema | null {
  if (isObject(value) && typeof value.getInputJsonSchema === "function") {
    const schema = (value.getInputJsonSchema as () => unknown).call(value);
    if (isJsonSchema(schema)) return schema;
  }
  return schemaFrom(value);
}

function annotationSchema(definition: unknown): JsonSchema | null {
  if (!isObject(definition)) return null;
  const properties: Record<string, JsonSchema> = {};
  for (const [key, channel] of Object.entries(definition)) {
    if (key.startsWith("__")) continue;
    const channelRecord = isObject(channel) ? channel : {};
    // Annotation<T> erases T at runtime. MessagesAnnotation keeps a named
    // reducer, so its array shape is one case we can describe precisely.
    properties[key] = key === "messages" &&
      channelRecord.lc_graph_name === "BinaryOperatorAggregate"
      ? { type: "array", items: { type: "object", properties: {
        type: { type: "string" }, content: {}, id: { type: "string" },
      }, additionalProperties: true } }
      : {};
  }
  return { type: "object", properties, additionalProperties: true };
}

function callSchema(graph: GraphLike, method: keyof GraphLike): JsonSchema | null {
  const candidate = graph[method];
  if (typeof candidate !== "function") return null;
  try { return schemaFrom((candidate as () => unknown).call(graph)); }
  catch { return null; }
}

function topology(graph: GraphLike | GraphDefinition): GraphTopology {
  if ("getGraph" in graph && typeof graph.getGraph === "function") {
    const drawable = graph.getGraph();
    const result = drawable.toJSON?.() ?? drawable;
    if (isObject(result) && Array.isArray(result.nodes) && Array.isArray(result.edges)) {
      return { nodes: result.nodes as Array<Record<string, unknown>>,
        edges: result.edges as Array<Record<string, unknown>> };
    }
  }
  if ("nodes" in graph && isObject(graph.nodes)) {
    const ids = Object.keys(graph.nodes);
    const edges = [{ source: "__start__", target: graph.entrypoint },
      ...Object.entries(graph.edges ?? {}).flatMap(([source, destinations]) =>
        (Array.isArray(destinations) ? destinations : [destinations]).map(target => ({ source, target })))];
    return { nodes: ["__start__", ...ids, "__end__"].map(id => ({ id })), edges };
  }
  return { nodes: [], edges: [] };
}

function schemas(graph: GraphLike | GraphDefinition): GraphSchemas {
  if ("nodes" in graph && !("getGraph" in graph && typeof graph.getGraph === "function")) {
    const generic = emptySchema();
    return { input_schema: generic, output_schema: generic, state_schema: generic, config_schema: generic };
  }
  const compiled = graph as GraphLike;
  const builder = compiled.builder ?? {};
  const stateRuntime = builder._schemaRuntimeDefinition;
  const state = callSchema(compiled, "getStateSchema") ?? schemaFrom(stateRuntime) ??
    annotationSchema(builder._schemaDefinition) ?? emptySchema();
  const input = callSchema(compiled, "getInputSchema") ??
    inputSchemaFrom(builder._inputRuntimeDefinition) ?? inputSchemaFrom(stateRuntime) ??
    annotationSchema(builder._inputDefinition) ?? state;
  const output = callSchema(compiled, "getOutputSchema") ??
    schemaFrom(builder._outputRuntimeDefinition) ??
    annotationSchema(builder._outputDefinition) ?? state;
  const config = callSchema(compiled, "getConfigSchema") ??
    schemaFrom(builder._configRuntimeSchema) ?? emptySchema();
  return { input_schema: input, output_schema: output, state_schema: state, config_schema: config };
}

/** Use public LangGraph graph/subgraph APIs; fall back to runtime schema metadata. */
export function describeGraph(value: unknown): GraphDescription {
  const graph = value as GraphLike | GraphDefinition;
  const subgraphs: GraphDescription["subgraphs"] = {};
  if ("getSubgraphs" in graph && typeof graph.getSubgraphs === "function") {
    for (const [name, child] of graph.getSubgraphs(undefined, true)) {
      subgraphs[name] = { graph: topology(child), schemas: schemas(child) };
    }
  }
  return { graph: topology(graph), schemas: schemas(graph), subgraphs };
}
