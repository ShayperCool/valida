import { BaseStore, type Item, type Operation, type OperationResults } from "@langchain/langgraph-checkpoint";
import type { ApiRequestContext, JsonRecord, PlatformAdapter } from "../api/types.ts";
import type { Store } from "../db/index.ts";
import { createStoreExtension, type StoreOptions } from "./store.ts";

type Backend = NonNullable<PlatformAdapter["store"]>;
const internal = { request: new Request("http://localhost/internal/langgraph-store") };
const item = (row: JsonRecord): Item => ({ namespace: row.namespace as string[], key: String(row.key),
  value: row.value as JsonRecord, createdAt: new Date(String(row.created_at)), updatedAt: new Date(String(row.updated_at)) });
const equal = (a: unknown, b: unknown) => JSON.stringify(a) === JSON.stringify(b);
function matches(value: JsonRecord, filter: JsonRecord = {}) {
  return Object.entries(filter).every(([key, expected]) => {
    const actual = value[key];
    if (!expected || typeof expected !== "object" || Array.isArray(expected)) return equal(actual, expected);
    const entries = Object.entries(expected);
    if (!entries.some(([op]) => op.startsWith("$"))) return equal(actual, expected);
    return entries.every(([op, right]) => {
      if (op === "$eq") return equal(actual, right);
      if (op === "$ne") return !equal(actual, right);
      if (!["$gt", "$gte", "$lt", "$lte"].includes(op)) throw new Error(`Unsupported store filter operator: ${op}`);
      if ((typeof actual !== "string" && typeof actual !== "number") || typeof actual !== typeof right) return false;
      const compared = right as typeof actual;
      return op === "$gt" ? actual > compared : op === "$gte" ? actual >= compared : op === "$lt" ? actual < compared : actual <= compared;
    });
  });
}

/** LangGraph BaseStore backed by the same durable, authorized items as Valida's Store API. */
export class ValidaBaseStore extends BaseStore {
  constructor(readonly backend: Backend, readonly context: ApiRequestContext = internal) { super(); }
  async batch<Op extends Operation[]>(operations: Op): Promise<OperationResults<Op>> {
    const results: unknown[] = [];
    for (const op of operations) {
      if ("value" in op) {
        if (op.value === null) await this.backend.delete(op.namespace, op.key, this.context);
        else await this.backend.put({ namespace: op.namespace, key: op.key, value: op.value, index: op.index }, this.context);
        results.push(undefined);
      } else if ("key" in op) {
        const found = await this.backend.get(op.namespace, op.key, this.context);
        results.push(found ? item(found) : null);
      } else if ("namespacePrefix" in op) {
        const rows: JsonRecord[] = [];
        for (let offset = 0;; offset += 1000) {
          const page = await this.backend.search({ namespace_prefix: op.namespacePrefix, query: op.query, limit: 1000, offset }, this.context);
          const found = page.items as JsonRecord[];
          rows.push(...found.filter(row => matches(row.value as JsonRecord, op.filter)));
          if (found.length < 1000 || rows.length >= (op.offset ?? 0) + (op.limit ?? 10)) break;
        }
        results.push(rows.slice(op.offset ?? 0, (op.offset ?? 0) + (op.limit ?? 10)).map(row => ({ ...item(row), ...(row.score === undefined ? {} : { score: Number(row.score) }) })));
      } else {
        const names: string[][] = [];
        for (let offset = 0;; offset += 1000) {
          const page = await this.backend.namespaces({ limit: 1000, offset }, this.context);
          const found = page.namespaces as string[][];
          names.push(...found);
          if (found.length < 1000) break;
        }
        const filtered = names.filter(ns => (op.matchConditions ?? []).every(match => {
          const start = match.matchType === "prefix" ? 0 : ns.length - match.path.length;
          return start >= 0 && match.path.every((part, i) => part === "*" ? start + i < ns.length : ns[start + i] === part);
        })).map(ns => op.maxDepth == null ? ns : ns.slice(0, op.maxDepth));
        const unique = [...new Map(filtered.map(ns => [JSON.stringify(ns), ns])).values()];
        results.push(unique.slice(op.offset, op.offset + op.limit));
      }
    }
    return results as OperationResults<Op>;
  }
}

export async function createLangGraphStore(store: Store, options: StoreOptions = {}, context?: ApiRequestContext) {
  return new ValidaBaseStore(await createStoreExtension(store, options), context);
}
