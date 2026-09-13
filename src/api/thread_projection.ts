import { ApiError, type JsonRecord, type Thread } from "./types.ts";

const selectableFields = new Set([
  "thread_id", "created_at", "updated_at", "state_updated_at", "metadata",
  "config", "context", "status", "values", "interrupts",
]);
const extractRoots = new Set(["values", "metadata", "config", "interrupts"]);
type PathPart = string | number;

function parsePath(path: string): PathPart[] {
  const source = path.startsWith("$.") ? path.slice(2) : path;
  const root = /^[a-z_][a-z_0-9]*/i.exec(source)?.[0];
  if (!root || !extractRoots.has(root)) {
    throw new ApiError(422, `Invalid extract path '${path}'`);
  }
  const parts: PathPart[] = [root];
  let position = root.length;
  while (position < source.length) {
    const remaining = source.slice(position);
    const property = /^\.([a-z_][a-z_0-9]*)/i.exec(remaining);
    if (property) {
      parts.push(property[1]!);
      position += property[0].length;
      continue;
    }
    const index = /^\[(-?\d+)\]/.exec(remaining);
    if (index) {
      parts.push(Number(index[1]));
      position += index[0].length;
      continue;
    }
    const bracketProperty = /^\[("(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*')\]/.exec(remaining);
    if (bracketProperty) {
      const quoted = bracketProperty[1]!;
      try {
        parts.push(quoted[0] === '"' ? JSON.parse(quoted) : quoted.slice(1, -1).replace(/\\'/g, "'"));
      } catch {
        throw new ApiError(422, `Invalid extract path '${path}'`);
      }
      position += bracketProperty[0].length;
      continue;
    }
    throw new ApiError(422, `Invalid extract path '${path}'`);
  }
  return parts;
}

function valueAt(source: unknown, parts: PathPart[]): unknown {
  let value = source;
  for (const part of parts) {
    if (typeof part === "number") {
      if (!Array.isArray(value)) return null;
      value = value[part < 0 ? value.length + part : part];
    } else {
      if (!value || typeof value !== "object" || !Object.hasOwn(value, part)) return null;
      value = (value as JsonRecord)[part];
    }
  }
  return value ?? null;
}

export interface ThreadProjection {
  select: Set<string> | null;
  extract: Map<string, PathPart[]> | null;
}

export function parseThreadProjection(query: JsonRecord): ThreadProjection {
  let select: Set<string> | null = null;
  if (query.select != null) {
    if (!Array.isArray(query.select) || !query.select.every(
      field => typeof field === "string" && selectableFields.has(field))) {
      throw new ApiError(422, "select must contain supported thread fields");
    }
    select = new Set(query.select);
  }
  let extract: Map<string, PathPart[]> | null = null;
  if (query.extract != null) {
    if (typeof query.extract !== "object" || Array.isArray(query.extract) ||
      Object.keys(query.extract).length > 10) {
      throw new ApiError(422, "extract must map at most 10 aliases to paths");
    }
    extract = new Map();
    for (const [alias, path] of Object.entries(query.extract)) {
      if (!alias || typeof path !== "string" || path.length > 512) {
        throw new ApiError(422, "extract aliases and paths must be non-empty strings");
      }
      extract.set(alias, parsePath(path));
    }
  }
  return { select, extract };
}

export function projectThread(thread: Thread, projection: ThreadProjection): Thread {
  const projected = (projection.select === null
    ? { ...thread }
    : Object.fromEntries(Object.entries(thread).filter(([field]) => projection.select!.has(field)))) as Thread;
  if (projection.extract !== null) {
    const extracted: JsonRecord = Object.create(null);
    for (const [alias, path] of projection.extract) extracted[alias] = valueAt(thread, path);
    projected.extracted = extracted;
  }
  return projected;
}
