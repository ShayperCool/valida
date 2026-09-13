import assert from "node:assert/strict";
import { Client } from "@langchain/langgraph-sdk";
import { RemoteGraph } from "@langchain/langgraph/remote";
import { Command } from "@langchain/langgraph";

const primaryUrl = process.env.VALIDA_API_URL ?? "http://127.0.0.1:2026";
const secondaryUrl = process.env.VALIDA_API_URL_2 ?? primaryUrl;
const primary = new Client({ apiUrl: primaryUrl, apiKey: null });
const secondary = new Client({ apiUrl: secondaryUrl, apiKey: null });

function record(value: unknown): Record<string, unknown> {
  assert.ok(value && typeof value === "object" && !Array.isArray(value));
  return value as Record<string, unknown>;
}

for (const url of new Set([primaryUrl, secondaryUrl])) {
  const response = await fetch(new URL("/health", url));
  assert.equal(response.status, 200, `Valida is unavailable at ${url}`);
}

const assistants = await secondary.assistants.search();
for (const id of ["echo", "counter", "approval"]) {
  assert.ok(assistants.some(assistant => assistant.assistant_id === id), `${id} assistant is missing`);
}

const counter = await primary.threads.create();
const counterValues = record(await secondary.runs.wait(counter.thread_id, "counter", {
  input: { count: 7, increment: 5 },
}));
assert.equal(counterValues.count, 12);
assert.equal(record((await primary.threads.getState(counter.thread_id)).values).count, 12);

const chat = await secondary.threads.create();
const chatValues = record(await primary.runs.wait(chat.thread_id, "echo", {
  input: { messages: [{ role: "human", content: "Valida smoke test" }] },
}));
const messages = chatValues.messages;
assert.ok(Array.isArray(messages));
assert.equal(record(messages.at(-1)).content, "Echo: Valida smoke test");
const history = await secondary.threads.getHistory(chat.thread_id);
assert.ok(history.length > 1);
assert.ok(history[0]?.checkpoint?.checkpoint_id);

const approval = await primary.threads.create();
await secondary.runs.wait(approval.thread_id, "approval", { input: { proposal: "smoke" } });
const interrupted = await primary.threads.getState(approval.thread_id);
assert.ok(interrupted.next.length > 0);
assert.ok(interrupted.checkpoint?.checkpoint_id);
const approved = record(await primary.runs.wait(approval.thread_id, "approval", {
  command: { resume: { decisions: [{ type: "approve" }] } },
}));
assert.equal(approved.approved, true);
assert.equal(approved.result, "Approved: smoke");
assert.equal(record((await secondary.threads.getState(approval.thread_id)).values).result, "Approved: smoke");

const remote = new RemoteGraph({ graphId: "counter", client: secondary });
assert.equal(record(await remote.invoke({ count: 2, increment: 4 })).count, 6);
const statelessChunks: unknown[] = [];
for await (const chunk of await remote.stream({ count: 1, increment: 3 }, { streamMode: "values" })) {
  statelessChunks.push(chunk);
}
assert.equal(record(statelessChunks.at(-1)).count, 4);
const remoteThread = await primary.threads.create();
const remoteConfig = { configurable: { thread_id: remoteThread.thread_id } };
assert.equal(record(await remote.invoke({ count: 2, increment: 4 }, remoteConfig)).count, 6);
assert.equal(record((await primary.threads.getState(remoteThread.thread_id)).values).count, 6);
const statefulChunks: unknown[] = [];
for await (const chunk of await remote.stream({ increment: 2 }, { ...remoteConfig, streamMode: "values" })) {
  statefulChunks.push(chunk);
}
assert.equal(record(statefulChunks.at(-1)).count, 8);
assert.equal(record((await remote.getState(remoteConfig)).values).count, 8);
const remoteHistory = [];
for await (const snapshot of remote.getStateHistory(remoteConfig)) remoteHistory.push(snapshot);
assert.ok(remoteHistory.length > 2);
assert.ok(remoteHistory[0]?.config.configurable?.checkpoint_id);

const remoteApproval = new RemoteGraph({ graphId: "approval", client: secondary });
const approvalThread = await primary.threads.create();
const approvalConfig = { configurable: { thread_id: approvalThread.thread_id } };
await assert.rejects(remoteApproval.invoke({ proposal: "RemoteGraph smoke" }, approvalConfig));
assert.ok((await remoteApproval.getState(approvalConfig)).next.length > 0);
assert.equal(record(await remoteApproval.invoke(new Command({
  resume: { decisions: [{ type: "approve" }] },
}), approvalConfig)).approved, true);

const v2 = secondary.threads.stream({ assistantId: "echo",
  maxReconnectAttempts: 0, streamIdleReconnect: 0 });
try {
  await v2.run.start({ input: { messages: [{ role: "human", content: "v2 smoke" }] } });
  const values = record(await v2.values);
  const messages = values.messages;
  assert.ok(Array.isArray(messages));
  assert.equal(record(messages.at(-1)).content, "Echo: v2 smoke");
} finally {
  await v2.close();
}

console.log(JSON.stringify({
  status: "passed", primary: primaryUrl, secondary: secondaryUrl,
  checks: ["assistants", "counter", "cross-instance state", "chat", "checkpoint history",
    "HITL resume", "RemoteGraph stateless invoke/stream", "RemoteGraph stateful checkpoint/history/stream",
    "RemoteGraph HITL resume", "v2 stream"],
}));
