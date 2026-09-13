CREATE TABLE IF NOT EXISTS `thread_ttl` (
	`thread_id` text PRIMARY KEY NOT NULL,
	`strategy` text NOT NULL,
	`ttl_minutes` real NOT NULL,
	`created_at` text NOT NULL,
	`expires_at` text NOT NULL,
	FOREIGN KEY (`thread_id`) REFERENCES `threads`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `thread_ttl_expires_at` ON `thread_ttl` (`expires_at`);
