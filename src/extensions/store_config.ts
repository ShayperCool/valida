import { loadModuleRef, type ServerConfig } from "../config.ts";
import type { EmbeddingProvider, StoreIndexConfig } from "./store.ts";

type IndexReference = NonNullable<NonNullable<ServerConfig["store"]>["index"]>;

/** Resolve a Valida config module reference such as ./embeddings.ts:embed. */
export async function loadStoreIndex(
  reference: IndexReference | undefined,
  configDirectory: string,
): Promise<StoreIndexConfig | undefined> {
  if (!reference) return undefined;
  if (!Number.isSafeInteger(reference.dims) || reference.dims <= 0) {
    throw new Error("store.index.dims must be a positive integer");
  }
  if (typeof reference.embed !== "string" || reference.embed.length === 0) {
    throw new Error("store.index.embed must be a TypeScript module reference");
  }
  const embed = await loadModuleRef<unknown>(reference.embed, configDirectory);
  if (typeof embed !== "function") throw new Error("store.index.embed must export a function");
  return { dims: reference.dims, embed: embed as EmbeddingProvider, fields: reference.fields };
}
