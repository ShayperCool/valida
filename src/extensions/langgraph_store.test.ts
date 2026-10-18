import { expect, test } from "bun:test";
import { createRuntime } from "../engine/index.ts";
import { createLangGraphStore } from "./langgraph_store.ts";
import { createStoreExtension } from "./store.ts";

test("LangGraph BaseStore shares durable API items and supports native batch operations", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite", url: ":memory:" } });
  try {
    const store = await createLangGraphStore(runtime.store);
    const api = await createStoreExtension(runtime.store);
    const ctx = { request: new Request("http://localhost") };
    await store.put(["users", "alice"], "profile", { kcal: 2200 });
    expect((await api.get(["users", "alice"], "profile", ctx))?.value).toEqual({ kcal: 2200 });
    expect((await store.get(["users", "alice"], "profile"))?.createdAt).toBeInstanceOf(Date);
    await store.put(["users", "bob"], "profile", { kcal: 1800 });
    expect((await store.search(["users"], { filter: { kcal: { $gte: 2000 } } })).map(item => item.namespace)).toEqual([["users", "alice"]]);
    expect(await store.listNamespaces({ prefix: ["users", "*"], maxDepth: 1 })).toEqual([["users"]]);
    const results = await store.batch([{ namespace: ["users", "alice"], key: "fact", value: { text: "vegan" } }, { namespace: ["users", "alice"], key: "fact" }]);
    expect(results[1]?.value).toEqual({ text: "vegan" });
    await store.delete(["users", "alice"], "profile");
    expect(await store.get(["users", "alice"], "profile")).toBeNull();
    expect((await store.search(["users", "bob"]))).toHaveLength(1);
  } finally { await runtime.close(); }
});
