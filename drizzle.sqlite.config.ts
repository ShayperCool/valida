import { defineConfig } from "drizzle-kit";

export default defineConfig({
  dialect: "sqlite",
  schema: "./src/db/schema.sqlite.ts",
  out: "./drizzle/sqlite",
  dbCredentials: { url: process.env.DATABASE_URL?.replace(/^file:/, "") || "./data/valida.db" },
});
