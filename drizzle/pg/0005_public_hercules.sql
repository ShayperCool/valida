CREATE TABLE IF NOT EXISTS "thread_ttl" (
	"thread_id" text PRIMARY KEY NOT NULL,
	"strategy" text NOT NULL,
	"ttl_minutes" double precision NOT NULL,
	"created_at" text NOT NULL,
	"expires_at" text NOT NULL
);
--> statement-breakpoint
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'thread_ttl'::regclass AND contype = 'f') THEN
    ALTER TABLE "thread_ttl" ADD CONSTRAINT "thread_ttl_thread_id_threads_id_fk"
      FOREIGN KEY ("thread_id") REFERENCES "public"."threads"("id") ON DELETE cascade ON UPDATE no action;
  END IF;
END $$;--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "thread_ttl_expires_at" ON "thread_ttl" USING btree ("expires_at");
