import { existsSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { dirname, isAbsolute, resolve } from "node:path";
import { pathToFileURL } from "node:url";

export interface ServerConfig {
  graphs: Record<string, string>;
  auth?: { path: string; disable_studio_auth?: boolean };
  http?: {
    app?: string;
    middleware?: string | string[];
    middleware_order?: "auth_first" | "middleware_first";
    enable_custom_route_auth?: boolean;
    cors?: { allow_origins?: string[]; allow_credentials?: boolean };
  };
  execution?: { mode?: "standalone" | "distributed"; concurrency?: number };
}

export interface LoadedConfig {
  path: string;
  directory: string;
  value: ServerConfig;
}

export async function loadConfig(path = process.env.VALIDA_CONFIG): Promise<LoadedConfig> {
  const chosen = path ?? (existsSync("valida.json") ? "valida.json" : "langgraph.json");
  const absolute = resolve(chosen);
  const value = JSON.parse(await readFile(absolute, "utf8")) as ServerConfig;
  if (!value.graphs || typeof value.graphs !== "object" || Array.isArray(value.graphs)) {
    throw new Error(`${chosen}: graphs must be an object`);
  }
  return { path: absolute, directory: dirname(absolute), value };
}

export async function loadModuleRef<T = unknown>(ref: string, configDirectory: string): Promise<T> {
  const separator = ref.lastIndexOf(":");
  const file = separator < 0 ? ref : ref.slice(0, separator);
  const exportName = separator < 0 ? "default" : ref.slice(separator + 1);
  const specifier = file.startsWith(".") || isAbsolute(file)
    ? pathToFileURL(resolve(configDirectory, file)).href
    : file;
  const module = await import(specifier);
  if (!(exportName in module)) throw new Error(`Export ${exportName} not found in ${file}`);
  return module[exportName] as T;
}
