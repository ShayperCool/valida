import { expect, test } from "bun:test";
import { resolveThreadTtlPolicy, threadTtlForRequest } from "./thread_ttl_config.ts";

test("thread TTL is opt-in and resolves minute defaults with environment precedence", () => {
  expect(resolveThreadTtlPolicy(undefined, {})).toBeNull();
  expect(resolveThreadTtlPolicy({ ttl: {} }, {})).toEqual({
    defaultTtlMinutes: 43_200, strategy: "delete", sweepIntervalMs: 300_000, sweepLimit: 1_000,
  });
  expect(resolveThreadTtlPolicy({ ttl: { default_ttl: 10, strategy: "keep_latest" } },
    { VALIDA_THREAD_TTL: "2.5" })).toEqual({
    defaultTtlMinutes: 2.5, strategy: "delete", sweepIntervalMs: 300_000, sweepLimit: 1_000,
  });
  expect(resolveThreadTtlPolicy(undefined, { LANGGRAPH_THREAD_TTL: JSON.stringify({
    default_ttl: null, strategy: "keep_latest", sweep_interval_minutes: 1, sweep_limit: 50,
  }) })).toEqual({
    defaultTtlMinutes: null, strategy: "keep_latest", sweepIntervalMs: 60_000, sweepLimit: 50,
  });
  expect(() => resolveThreadTtlPolicy(undefined, { VALIDA_THREAD_TTL: "0" })).toThrow();
  expect(() => resolveThreadTtlPolicy({ ttl: { default_ttl: Number.POSITIVE_INFINITY } }, {})).toThrow();
  expect(() => resolveThreadTtlPolicy(undefined, { VALIDA_THREAD_TTL: '{"strategy":"drop"}' })).toThrow();
});

test("request TTL accepts SDK and compatibility spellings but leaves ordinary threads untouched", () => {
  const policy = resolveThreadTtlPolicy({ ttl: { default_ttl: 30,
    strategy: "keep_latest" } }, {})!;
  expect(threadTtlForRequest(undefined, null, "create")).toBeUndefined();
  expect(threadTtlForRequest(undefined, policy, "create")).toEqual({
    ttlMinutes: 30, strategy: "keep_latest",
  });
  expect(threadTtlForRequest(undefined, policy, "update")).toBeUndefined();
  expect(threadTtlForRequest({ ttl: 5 }, null, "create")).toEqual({
    ttlMinutes: 5, strategy: "delete",
  });
  expect(threadTtlForRequest({ default_ttl: 2, strategy: "keep_latest" }, null, "create"))
    .toEqual({ ttlMinutes: 2, strategy: "keep_latest" });
  expect(threadTtlForRequest({ strategy: "delete" }, policy, "update"))
    .toEqual({ ttlMinutes: 30, strategy: "delete" });
  expect(() => threadTtlForRequest({ strategy: "delete" }, null, "create")).toThrow();
  expect(() => threadTtlForRequest({ ttl: 0 }, null, "create")).toThrow();
  expect(() => threadTtlForRequest({ ttl: "5" }, null, "create")).toThrow();
});
