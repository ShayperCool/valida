CREATE TABLE IF NOT EXISTS "valida_crons" (
	"cron_id" text PRIMARY KEY NOT NULL,
	"assistant_id" text NOT NULL,
	"thread_id" text,
	"schedule" text NOT NULL,
	"timezone" text NOT NULL,
	"enabled" integer NOT NULL,
	"payload" text NOT NULL,
	"metadata" text NOT NULL,
	"owner_id" text,
	"next_run_at" text,
	"end_time" text,
	"lease_until" text,
	"created_at" text NOT NULL,
	"updated_at" text NOT NULL
);
--> statement-breakpoint
CREATE TABLE IF NOT EXISTS "valida_store_items" (
	"namespace" text NOT NULL,
	"item_key" text NOT NULL,
	"item_value" text NOT NULL,
	"created_at" text NOT NULL,
	"updated_at" text NOT NULL,
	"expires_at" text,
	CONSTRAINT "valida_store_items_namespace_item_key_pk" PRIMARY KEY("namespace","item_key")
);
