CREATE TABLE "thread_ttl" (
	"thread_id" text PRIMARY KEY NOT NULL,
	"strategy" text NOT NULL,
	"ttl_minutes" double precision NOT NULL,
	"created_at" text NOT NULL,
	"expires_at" text NOT NULL
);
--> statement-breakpoint
ALTER TABLE "thread_ttl" ADD CONSTRAINT "thread_ttl_thread_id_threads_id_fk" FOREIGN KEY ("thread_id") REFERENCES "public"."threads"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
CREATE INDEX "thread_ttl_expires_at" ON "thread_ttl" USING btree ("expires_at");