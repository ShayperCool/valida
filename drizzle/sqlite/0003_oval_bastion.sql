CREATE TABLE IF NOT EXISTS `valida_store_embeddings` (
	`namespace` text NOT NULL,
	`item_key` text NOT NULL,
	`source_hash` text NOT NULL,
	`vectors` text NOT NULL,
	PRIMARY KEY(`namespace`, `item_key`)
);
