import { expect, test } from "bun:test";
import { sql } from "drizzle-orm";
import { createRuntime } from "../engine/index.ts";
import { createStoreExtension } from "./store.ts";
import { createCronExtension } from "./crons.ts";

test("JSON store persists exact keys, filters namespaces, and rejects unsupported vector queries", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite", url: ":memory:" } });
  try {
    const kv = await createStoreExtension(runtime.store);
    await kv.put({ namespace: ["users", "alice"], key: "profile", value: { role: "admin", age: 32 } }, { request: new Request("http://test") });
    await kv.put({ namespace: ["users", "bob"], key: "profile", value: { role: "viewer", age: 19 } }, { request: new Request("http://test") });
    expect((await kv.get(["users", "alice"], "profile", { request: new Request("http://test") }))?.value).toEqual({ role: "admin", age: 32 });
    const filtered = await kv.search({ namespace_prefix: ["users"], filter: { role: "admin" } }, { request: new Request("http://test") });
    expect(filtered.total).toBe(1);
    expect((filtered.items as Array<{ namespace: string[] }>)[0]?.namespace).toEqual(["users", "alice"]);
    expect((await kv.namespaces({ prefix: ["users"] }, { request: new Request("http://test") })).namespaces).toEqual([
      ["users", "alice"], ["users", "bob"],
    ]);
    await expect(kv.search({ namespace_prefix: [], query: "admin" }, { request: new Request("http://test") })).rejects.toMatchObject({ status: 501 });
    await kv.delete(["users", "alice"], "profile", { request: new Request("http://test") });
    expect(await kv.get(["users", "alice"], "profile", { request: new Request("http://test") })).toBeNull();
  } finally {
    await runtime.close();
  }
});

test("cron extension fires immediately and SQL claims a due firing once across schedulers", async () => {
  const runtime = await createRuntime({ db: { dialect: "sqlite", url: ":memory:" } });
  try {
    runtime.registerGraph({ id: "counter", entrypoint: "add", nodes: { add: state => ({ count: Number(state.count ?? 0) + 1 }) } });
    await runtime.store.createAssistant({ id: "counter", graphId: "counter", name: "counter" });
    const first = await createCronExtension(runtime.store, runtime);
    const second = await createCronExtension(runtime.store, runtime);
    const context = { request: new Request("http://test") };
    const immediate = await first.create(null, {
      assistant_id: "counter", schedule: "*/5 * * * *", timezone: "UTC", input: { count: 3 },
    }, context);
    expect(immediate.run_id).toBeTruthy();
    expect((await runtime.waitRun(String(immediate.run_id))).output).toMatchObject({ count: 4 });
    const created = (await first.search({ assistant_id: "counter" }, context))[0]!;
    expect(created.next_run_date).toBeTruthy();
    expect(await first.count({ assistant_id: "counter" }, context)).toBe(1);
    await runtime.store.exec(sql`UPDATE valida_crons SET next_run_at = ${"2020-01-01T00:00:00.000Z"}
      WHERE cron_id = ${String(created.cron_id)}`);
    expect((await Promise.all([first.tick(), second.tick()])).reduce((sum, value) => sum + value, 0)).toBe(1);
    expect((await first.search({}, context))[0]?.next_run_date).not.toBe("2020-01-01T00:00:00.000Z");
    const disabled = await first.update(String(created.cron_id), { enabled: false }, context);
    expect(disabled?.enabled).toBe(false);
    expect(disabled?.next_run_date).toBeNull();
    expect(await first.delete(String(created.cron_id), context)).toBe(true);
    expect(await first.count({}, context)).toBe(0);
  } finally {
    await runtime.close();
  }
});
