CREATE TABLE IF NOT EXISTS `assistants` (
	`id` text PRIMARY KEY NOT NULL,
	`graph_id` text NOT NULL,
	`name` text NOT NULL,
	`description` text,
	`config` text NOT NULL,
	`metadata` text NOT NULL,
	`created_at` text NOT NULL,
	`updated_at` text NOT NULL
);
--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `checkpoints` (
	`id` text PRIMARY KEY NOT NULL,
	`thread_id` text NOT NULL,
	`run_id` text NOT NULL,
	`graph_id` text NOT NULL,
	`step` integer NOT NULL,
	`state_values` text NOT NULL,
	`next` text NOT NULL,
	`tasks` text NOT NULL,
	`interrupts` text NOT NULL,
	`parent_id` text,
	`created_at` text NOT NULL
);
--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `events` (
	`run_id` text NOT NULL,
	`seq` integer NOT NULL,
	`event` text NOT NULL,
	`data` text NOT NULL,
	`created_at` text NOT NULL,
	PRIMARY KEY(`run_id`, `seq`)
);
--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `lg_checkpoints` (
	`thread_id` text NOT NULL,
	`checkpoint_ns` text NOT NULL,
	`checkpoint_id` text NOT NULL,
	`parent_id` text,
	`checkpoint_type` text NOT NULL,
	`checkpoint_blob` text NOT NULL,
	`metadata_type` text NOT NULL,
	`metadata_blob` text NOT NULL,
	`created_at` text NOT NULL,
	PRIMARY KEY(`thread_id`, `checkpoint_ns`, `checkpoint_id`)
);
--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `lg_writes` (
	`thread_id` text NOT NULL,
	`checkpoint_ns` text NOT NULL,
	`checkpoint_id` text NOT NULL,
	`task_id` text NOT NULL,
	`write_idx` integer NOT NULL,
	`channel` text NOT NULL,
	`value_type` text NOT NULL,
	`value_blob` text NOT NULL,
	PRIMARY KEY(`thread_id`, `checkpoint_ns`, `checkpoint_id`, `task_id`, `write_idx`)
);
--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `runs` (
	`id` text PRIMARY KEY NOT NULL,
	`thread_id` text NOT NULL,
	`assistant_id` text,
	`graph_id` text NOT NULL,
	`status` text NOT NULL,
	`input` text,
	`output` text,
	`error` text,
	`config` text NOT NULL,
	`metadata` text NOT NULL,
	`resume` text,
	`lease_until` text,
	`created_at` text NOT NULL,
	`updated_at` text NOT NULL
);
--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `threads` (
	`id` text PRIMARY KEY NOT NULL,
	`metadata` text NOT NULL,
	`status` text NOT NULL,
	`created_at` text NOT NULL,
	`updated_at` text NOT NULL
);
