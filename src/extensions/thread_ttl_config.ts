import type { ServerConfig } from "../config.ts";
import type { ThreadTtlSpec, ThreadTtlStrategy } from "../db/index.ts";
import { ApiError } from "../api/types.ts";

export interface ThreadTtlPolicy {
  defaultTtlMinutes: number | null;
  strategy: ThreadTtlStrategy;
  sweepIntervalMs: number;
  sweepLimit: number;
}

const MAX_TTL_MINUTES = 1_000_000_000;
type Source = NonNullable<NonNullable<ServerConfig["checkpointer"]>["ttl"]>;

function positiveMinutes(value: unknown, name: string, minimum = 0): number {
  if (typeof value !== "number" || !Number.isFinite(value) || value <= minimum || value > MAX_TTL_MINUTES) {
    throw new Error(`${name} must be a finite number greater than ${minimum} and at most ${MAX_TTL_MINUTES} minutes`);
  }
  return value;
}

/** Environment overrides the whole config block; no source means no default TTL. */
export function resolveThreadTtlPolicy(
  config: ServerConfig["checkpointer"], env: Record<string, string | undefined> = process.env,
): ThreadTtlPolicy | null {
  const raw = env.VALIDA_THREAD_TTL?.trim() || env.LANGGRAPH_THREAD_TTL?.trim();
  let source: Source | null = config?.ttl ?? null;
  if (raw) {
    const numeric = Number(raw);
    if (Number.isFinite(numeric)) source = { default_ttl: numeric };
    else {
      try { source = JSON.parse(raw) as Source; }
      catch { throw new Error("VALIDA_THREAD_TTL must be a minute count or JSON object"); }
    }
  }
  if (source === null) return null;
  if (!source || typeof source !== "object" || Array.isArray(source)) {
    throw new Error("Thread TTL config must be an object");
  }
  const defaultTtlMinutes = source.default_ttl === null ? null
    : positiveMinutes(source.default_ttl ?? 43_200, "default_ttl");
  const strategy = source.strategy ?? "delete";
  if (strategy !== "delete" && strategy !== "keep_latest") {
    throw new Error("Thread TTL strategy must be delete or keep_latest");
  }
  const sweepIntervalMinutes = positiveMinutes(source.sweep_interval_minutes ?? 5,
    "sweep_interval_minutes", 1 / 60 - Number.EPSILON);
  const sweepLimit = source.sweep_limit ?? 1_000;
  if (!Number.isSafeInteger(sweepLimit) || sweepLimit < 1 || sweepLimit > 10_000) {
    throw new Error("sweep_limit must be an integer from 1 to 10000");
  }
  return { defaultTtlMinutes, strategy, sweepIntervalMs: sweepIntervalMinutes * 60_000, sweepLimit };
}

/** Parse SDK `{ttl, strategy}` and the `default_ttl` compatibility alias. */
export function threadTtlForRequest(
  requested: unknown, policy: ThreadTtlPolicy | null, mode: "create" | "update",
): ThreadTtlSpec | null | undefined {
  if (requested === undefined || (requested === null && mode === "create")) {
    return mode === "create" && policy?.defaultTtlMinutes != null
      ? { ttlMinutes: policy.defaultTtlMinutes, strategy: policy.strategy } : undefined;
  }
  if (requested === null) return null;
  const value = typeof requested === "number" ? { ttl: requested } : requested;
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new ApiError(422, "ttl must be a minute count or object");
  }
  const spec = value as Record<string, unknown>;
  const minutes = spec.default_ttl ?? spec.ttl ?? policy?.defaultTtlMinutes;
  if (typeof minutes !== "number" || !Number.isFinite(minutes) ||
    minutes <= 0 || minutes > MAX_TTL_MINUTES) {
    throw new ApiError(422, "ttl must be greater than 0 and at most 1000000000 minutes");
  }
  const strategy = spec.strategy ?? policy?.strategy ?? "delete";
  if (strategy !== "delete" && strategy !== "keep_latest") {
    throw new ApiError(422, "ttl strategy must be delete or keep_latest");
  }
  return { ttlMinutes: minutes, strategy };
}
