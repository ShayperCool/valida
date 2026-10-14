import { defineConfig } from "vitest/config";
export default defineConfig({ test: { globals: true, include: ["conformance/**/*.conformance.ts"], pool: "forks" } });
