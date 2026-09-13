CREATE EXTENSION IF NOT EXISTS vector;
--> statement-breakpoint
CREATE TABLE IF NOT EXISTS "valida_store_vectors" (
	"namespace" text NOT NULL,
	"item_key" text NOT NULL,
	"field" text NOT NULL,
	"source_hash" text NOT NULL,
	"dims" integer NOT NULL,
	"embedding" vector NOT NULL,
	CONSTRAINT "valida_store_vectors_namespace_item_key_field_pk" PRIMARY KEY("namespace","item_key","field")
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "valida_store_vectors_hnsw_1536" ON "valida_store_vectors" USING hnsw ((embedding::vector(1536)) vector_cosine_ops) WHERE dims = 1536;
