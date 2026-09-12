import type { Hono } from "hono";
import type { MiddlewareHandler } from "hono";
import { loadModuleRef, type LoadedConfig } from "./config.ts";

export type PluginMiddleware = MiddlewareHandler | MiddlewareHandler[];

export async function loadMiddleware(config: LoadedConfig): Promise<MiddlewareHandler[]> {
  const entry = config.value.http?.middleware;
  if (!entry) return [];
  const refs = Array.isArray(entry) ? entry : [entry];
  const handlers: MiddlewareHandler[] = [];
  for (const ref of refs) {
    const loaded = await loadModuleRef<PluginMiddleware>(ref, config.directory);
    const values = Array.isArray(loaded) ? loaded : [loaded];
    for (const handler of values) {
      if (typeof handler !== "function") throw new Error(`Middleware ${ref} must export a Hono handler`);
      handlers.push(handler);
    }
  }
  return handlers;
}

export async function loadCustomApp(config: LoadedConfig): Promise<Hono | null> {
  const ref = config.value.http?.app;
  if (!ref) return null;
  const app = await loadModuleRef<Hono>(ref, config.directory);
  if (!app || typeof app.fetch !== "function") {
    throw new Error(`Custom app ${ref} must export a Hono instance`);
  }
  return app;
}
