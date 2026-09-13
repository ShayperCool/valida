CREATE TABLE IF NOT EXISTS "valida_store_embeddings" (
	"namespace" text NOT NULL,
	"item_key" text NOT NULL,
	"source_hash" text NOT NULL,
	"vectors" text NOT NULL,
	CONSTRAINT "valida_store_embeddings_namespace_item_key_pk" PRIMARY KEY("namespace","item_key")
);
