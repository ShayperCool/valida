CREATE TABLE IF NOT EXISTS "valida_assistant_heads" (
	"assistant_id" text PRIMARY KEY NOT NULL,
	"version" integer NOT NULL
);
--> statement-breakpoint
CREATE TABLE IF NOT EXISTS "valida_assistant_versions" (
	"assistant_id" text NOT NULL,
	"version" integer NOT NULL,
	"snapshot" text NOT NULL,
	"created_at" text NOT NULL,
	CONSTRAINT "valida_assistant_versions_assistant_id_version_pk" PRIMARY KEY("assistant_id","version")
);
