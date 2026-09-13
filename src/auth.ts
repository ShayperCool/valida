import { AsyncLocalStorage } from "node:async_hooks";
import type { MiddlewareHandler } from "hono";
import type { LoadedConfig } from "./config.ts";
import { loadModuleRef } from "./config.ts";

type AuthValue = Record<string, unknown>;

export interface AuthUser {
  identity: string;
  is_authenticated?: boolean;
  display_name?: string;
  permissions?: string[];
  [key: string]: unknown;
}

export interface AuthContext {
  user: AuthUser;
  resource: string;
  action: string;
  permissions: string[];
  path: string;
  method: string;
  params: Record<string, string>;
  query: AuthValue;
}

/** Return a filter for reads/searches, or a replacement payload for writes. */
export type AuthDecision = boolean | AuthValue | void;

export interface AuthProvider {
  authenticate: (request: Request) => AuthUser | Promise<AuthUser>;
  authorize?: (context: AuthContext, value: AuthValue) => AuthDecision | Promise<AuthDecision>;
}

/** Effective authorization data for API handlers and adapters in this request. */
export interface AuthorizationState extends AuthContext {
  /** The value passed to authorize, including any in-place changes made by it. */
  value: AuthValue;
  /** Effective JSON body for create/update/create_run; null for other actions. */
  payload: AuthValue | null;
  /** Effective per-run payloads for POST /runs/batch after individual authorization. */
  batchPayload?: AuthValue[];
  /** Restriction returned by authorize for search/read/delete; null if absent. */
  filter: AuthValue | null;
}

export const currentUser = new AsyncLocalStorage<AuthUser>();
export const currentAuthorization = new AsyncLocalStorage<AuthorizationState>();

export async function loadAuth(config: LoadedConfig): Promise<AuthProvider | null> {
  if (!config.value.auth?.path) return null;
  const handler = await loadModuleRef<AuthProvider | AuthProvider["authenticate"]>(
    config.value.auth.path, config.directory,
  );
  const provider = typeof handler === "function" ? { authenticate: handler } : handler;
  if (typeof provider.authenticate !== "function") {
    throw new Error("Auth module must export authenticate(request) or an AuthProvider");
  }
  return provider;
}

interface AuthTarget {
  resource: string;
  action: string;
  params: Record<string, string>;
}

/** Resolve Agent Protocol routes before broad run/threads rules. */
export function routeAuthTarget(method: string, path: string): AuthTarget | null {
  const segments = path.split("/").filter(Boolean);
  const [root, second, third, fourth, fifth] = segments;
  const params: Record<string, string> = {};
  const target = (resource: string, action: string): AuthTarget => ({ resource, action, params });
  const queryAction = (part: string | undefined): string | null =>
    part === "search" || part === "count" ? "search" : null;

  if (root === "ag-ui" && second && !third && method === "POST") {
    params.assistant_id = second;
    return target("threads", "create_run");
  }

  if (root === "runs" && second === "crons") {
    if (fourth || fifth) return null;
    if (third && !queryAction(third)) params.cron_id = third;
    if (method === "POST") return target("crons", queryAction(third) ?? "create");
    if (method === "GET") return target("crons", third ? "read" : "search");
    if (method === "PATCH") return target("crons", "update");
    if (method === "DELETE") return target("crons", "delete");
    return null;
  }

  if (root === "assistants") {
    if (second && !queryAction(second)) params.assistant_id = second;
    if (method === "GET") return target("assistants", second ? "read" : "search");
    if (method === "POST") {
      if (queryAction(second) || third === "versions") return target("assistants", "search");
      if (third === "latest") return target("assistants", "update");
      return target("assistants", "create");
    }
    if (method === "PATCH") return target("assistants", "update");
    if (method === "DELETE") return target("assistants", "delete");
    return null;
  }

  if (root === "threads") {
    if (second && !["search", "count", "prune"].includes(second)) params.thread_id = second;
    if (third === "runs" && fourth === "crons") {
      return method === "POST" ? target("crons", "create") : null;
    }
    if (third === "runs") {
      if (fourth && !["stream", "wait"].includes(fourth)) params.run_id = fourth;
      if (method === "POST") return target("threads", fourth === "cancel" || fifth === "cancel" ? "update" : "create_run");
      if (method === "GET") return target("threads", fourth ? "read" : "search");
      if (method === "PATCH") return target("threads", "update");
      if (method === "DELETE") return target("threads", "delete");
      return null;
    }
    if (third === "commands" || (third === "stream" && fourth === "events")) {
      return method === "POST" ? target("threads", "create_run") : null;
    }
    if (method === "GET") return target("threads", second ? "read" : "search");
    if (method === "POST") {
      if (queryAction(second)) return target("threads", "search");
      if (second === "prune") return target("threads", "delete");
      if (third === "copy") return target("threads", "create");
      if (third === "history" || (third === "state" && fourth === "checkpoint")) return target("threads", "read");
      if (third === "state") return target("threads", "update");
      return target("threads", "create");
    }
    if (method === "PATCH") return target("threads", "update");
    if (method === "DELETE") return target("threads", "delete");
    return null;
  }

  if (root === "runs") {
    if (second && !["stream", "wait", "batch"].includes(second)) params.run_id = second;
    if (method === "POST") return target("threads", "create_run");
    if (method === "GET") return target("threads", "read");
    return null;
  }

  if (root === "store") {
    if (method === "GET") return target("store", "read");
    if (method === "POST") return target("store", "search");
    if (method === "PUT") return target("store", "update");
    if (method === "DELETE") return target("store", "delete");
    return null;
  }
  return null;
}

function queryValue(url: URL): AuthValue {
  const result: AuthValue = {};
  for (const key of new Set(url.searchParams.keys())) {
    const values = url.searchParams.getAll(key);
    result[key] = values.length === 1 ? values[0] : values;
  }
  return result;
}

function isRecord(value: unknown): value is AuthValue {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

async function jsonBody(request: Request): Promise<unknown> {
  if (!request.headers.get("content-type")?.toLowerCase().includes("application/json")) return null;
  try {
    return await request.clone().json();
  } catch {
    // Let the route report malformed JSON using its normal error handling.
    return null;
  }
}

function isWrite(action: string): boolean {
  return action === "create" || action === "update" || action === "create_run";
}

export function authMiddleware(provider: AuthProvider | null, options: { protectCustomRoutes?: boolean } = {}): MiddlewareHandler {
  return async (context, next) => {
    if (!provider) return next();
    const url = new URL(context.req.url);
    const { pathname: path } = url;
    if (["/health", "/ready", "/live", "/info", "/openapi.json"].includes(path)) return next();
    const method = context.req.method.toUpperCase();
    const target = routeAuthTarget(method, path);
    if (!target && !options.protectCustomRoutes) return next();

    let user: AuthUser;
    try {
      user = await provider.authenticate(context.req.raw);
    } catch (error) {
      return context.json({ detail: error instanceof Error ? error.message : "Unauthorized" }, 401);
    }
    if (!user || !user.identity || user.is_authenticated === false) {
      return context.json({ detail: "Unauthorized" }, 401);
    }
    context.set("principal", user);

    const query = queryValue(url);
    const params = target?.params ?? {};
    const parsedBody = await jsonBody(context.req.raw);
    const requestBody = isRecord(parsedBody) ? parsedBody : null;
    const batchBody = path === "/runs/batch" && Array.isArray(parsedBody) && parsedBody.every(isRecord)
      ? parsedBody as AuthValue[] : null;
    const value: AuthValue = requestBody ?? { ...params, ...query };
    const authContext: AuthContext = {
      user, resource: target?.resource ?? "custom", action: target?.action ?? method.toLowerCase(),
      permissions: user.permissions ?? [], path, method, params, query,
    };
    let decision: AuthDecision = undefined;
    let batchPayload: AuthValue[] | undefined;
    if (batchBody) {
      batchPayload = [];
      for (const item of batchBody) {
        const itemDecision = await provider.authorize?.(authContext, item);
        if (itemDecision === false) return context.json({ detail: "Forbidden" }, 403);
        batchPayload.push(isRecord(itemDecision) ? itemDecision : item);
      }
    } else if (provider.authorize) {
      decision = await provider.authorize(authContext, value);
      if (decision === false) return context.json({ detail: "Forbidden" }, 403);
    }
    const replacement = isRecord(decision) ? decision : null;
    const state: AuthorizationState = {
      ...authContext, value,
      payload: target && isWrite(target.action) && requestBody !== null ? replacement ?? value : null,
      batchPayload,
      filter: target && !isWrite(target.action) ? replacement : null,
    };
    return currentUser.run(user, () => currentAuthorization.run(state, next));
  };
}
