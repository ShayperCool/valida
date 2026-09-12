import { AsyncLocalStorage } from "node:async_hooks";
import type { MiddlewareHandler } from "hono";
import type { LoadedConfig } from "./config.ts";
import { loadModuleRef } from "./config.ts";

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
}

export interface AuthProvider {
  authenticate: (request: Request) => AuthUser | Promise<AuthUser>;
  authorize?: (context: AuthContext, value: Record<string, unknown>) =>
    | boolean
    | Record<string, unknown>
    | void
    | Promise<boolean | Record<string, unknown> | void>;
}

export const currentUser = new AsyncLocalStorage<AuthUser>();

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

function routeAction(method: string, path: string): { resource: string; action: string } | null {
  const segments = path.split("/").filter(Boolean);
  const resource = segments[0];
  if (!["assistants", "threads", "runs", "store", "crons"].includes(resource ?? "")) return null;
  if (resource === "runs" || segments.includes("runs")) {
    return { resource: "threads", action: method === "POST" ? "create_run" : "read" };
  }
  if (method === "GET") return { resource, action: segments.length > 1 ? "read" : "search" };
  if (method === "DELETE") return { resource, action: "delete" };
  if (method === "PATCH" || method === "PUT") return { resource, action: "update" };
  if (method === "POST" && segments.at(-1) === "search") return { resource, action: "search" };
  if (method === "POST" && (segments.at(-1) === "state" || segments.at(-1) === "copy")) {
    return { resource, action: "update" };
  }
  return { resource, action: "create" };
}

export function authMiddleware(provider: AuthProvider | null): MiddlewareHandler {
  return async (context, next) => {
    if (!provider) return next();
    const path = new URL(context.req.url).pathname;
    if (["/health", "/ready", "/live", "/info", "/openapi.json"].includes(path)) return next();
    let user: AuthUser;
    try {
      user = await provider.authenticate(context.req.raw);
    } catch (error) {
      return context.json({ detail: error instanceof Error ? error.message : "Unauthorized" }, 401);
    }
    if (!user || !user.identity || user.is_authenticated === false) {
      return context.json({ detail: "Unauthorized" }, 401);
    }
    const target = routeAction(context.req.method, path);
    if (target && provider.authorize) {
      const decision = await provider.authorize(
        { user, ...target, permissions: user.permissions ?? [] },
        { path, method: context.req.method },
      );
      if (decision === false) return context.json({ detail: "Forbidden" }, 403);
    }
    return currentUser.run(user, next);
  };
}
