import { currentAuthorization } from "./auth.ts";
import type { JsonRecord } from "./api/types.ts";

function record(value: unknown): JsonRecord {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as JsonRecord : {};
}

/** Apply a handler's read/search/delete filter to a wire-format resource. */
export function matchesAuthorizationFilter(resource: string, value: unknown): boolean {
  const state = currentAuthorization.getStore();
  if (state?.resource !== resource || !state.filter) return true;
  const matches = (candidate: unknown, filter: JsonRecord): boolean => {
    const actual = record(candidate);
    return Object.entries(filter).every(([key, expected]) => {
      const current = actual[key];
      if (expected && typeof expected === "object" && !Array.isArray(expected)) {
        return matches(current, expected as JsonRecord);
      }
      return JSON.stringify(current) === JSON.stringify(expected);
    });
  };
  return matches(value, state.filter);
}
