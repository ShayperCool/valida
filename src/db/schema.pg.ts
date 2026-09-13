import { integer, pgTable, primaryKey, text } from "drizzle-orm/pg-core";

export const assistants = pgTable("assistants", {
  id: text("id").primaryKey(), graphId: text("graph_id").notNull(),
  name: text("name").notNull(), description: text("description"),
  config: text("config").notNull(), metadata: text("metadata").notNull(),
  createdAt: text("created_at").notNull(), updatedAt: text("updated_at").notNull(),
});
export const threads = pgTable("threads", {
  id: text("id").primaryKey(), metadata: text("metadata").notNull(),
  status: text("status").notNull(), createdAt: text("created_at").notNull(),
  updatedAt: text("updated_at").notNull(),
});
export const runs = pgTable("runs", {
  id: text("id").primaryKey(), threadId: text("thread_id").notNull(),
  assistantId: text("assistant_id"), graphId: text("graph_id").notNull(),
  status: text("status").notNull(), input: text("input"), output: text("output"),
  error: text("error"), config: text("config").notNull(), metadata: text("metadata").notNull(),
  resume: text("resume"), leaseUntil: text("lease_until"),
  createdAt: text("created_at").notNull(), updatedAt: text("updated_at").notNull(),
});
export const checkpoints = pgTable("checkpoints", {
  id: text("id").primaryKey(), threadId: text("thread_id").notNull(),
  runId: text("run_id").notNull(), graphId: text("graph_id").notNull(),
  step: integer("step").notNull(), values: text("state_values").notNull(),
  next: text("next").notNull(), tasks: text("tasks").notNull(),
  interrupts: text("interrupts").notNull(), parentId: text("parent_id"),
  createdAt: text("created_at").notNull(),
});
export const events = pgTable("events", {
  runId: text("run_id").notNull(), seq: integer("seq").notNull(),
  event: text("event").notNull(), data: text("data").notNull(),
  createdAt: text("created_at").notNull(),
}, table => [primaryKey({ columns: [table.runId, table.seq] })]);
export const lgCheckpoints = pgTable("lg_checkpoints", {
  threadId: text("thread_id").notNull(), namespace: text("checkpoint_ns").notNull(),
  checkpointId: text("checkpoint_id").notNull(), parentId: text("parent_id"),
  checkpointType: text("checkpoint_type").notNull(), checkpointBlob: text("checkpoint_blob").notNull(),
  metadataType: text("metadata_type").notNull(), metadataBlob: text("metadata_blob").notNull(),
  createdAt: text("created_at").notNull(),
}, table => [primaryKey({ columns: [table.threadId, table.namespace, table.checkpointId] })]);
export const lgWrites = pgTable("lg_writes", {
  threadId: text("thread_id").notNull(), namespace: text("checkpoint_ns").notNull(),
  checkpointId: text("checkpoint_id").notNull(), taskId: text("task_id").notNull(),
  writeIdx: integer("write_idx").notNull(), channel: text("channel").notNull(),
  valueType: text("value_type").notNull(), valueBlob: text("value_blob").notNull(),
}, table => [primaryKey({ columns: [table.threadId, table.namespace, table.checkpointId, table.taskId, table.writeIdx] })]);

export const storeItems = pgTable("valida_store_items", {
  namespace: text("namespace").notNull(), itemKey: text("item_key").notNull(),
  itemValue: text("item_value").notNull(), createdAt: text("created_at").notNull(),
  updatedAt: text("updated_at").notNull(), expiresAt: text("expires_at"),
}, table => [primaryKey({ columns: [table.namespace, table.itemKey] })]);

export const storeEmbeddings = pgTable("valida_store_embeddings", {
  namespace: text("namespace").notNull(), itemKey: text("item_key").notNull(),
  sourceHash: text("source_hash").notNull(), vectors: text("vectors").notNull(),
}, table => [primaryKey({ columns: [table.namespace, table.itemKey] })]);

export const crons = pgTable("valida_crons", {
  id: text("cron_id").primaryKey(), assistantId: text("assistant_id").notNull(),
  threadId: text("thread_id"), schedule: text("schedule").notNull(),
  timezone: text("timezone").notNull(), enabled: integer("enabled").notNull(),
  payload: text("payload").notNull(), metadata: text("metadata").notNull(),
  ownerId: text("owner_id"), nextRunAt: text("next_run_at"), endTime: text("end_time"),
  leaseUntil: text("lease_until"), createdAt: text("created_at").notNull(),
  updatedAt: text("updated_at").notNull(),
});

export const assistantVersions = pgTable("valida_assistant_versions", {
  assistantId: text("assistant_id").notNull(), version: integer("version").notNull(),
  snapshot: text("snapshot").notNull(), createdAt: text("created_at").notNull(),
}, table => [primaryKey({ columns: [table.assistantId, table.version] })]);

export const assistantHeads = pgTable("valida_assistant_heads", {
  assistantId: text("assistant_id").primaryKey(), version: integer("version").notNull(),
});
