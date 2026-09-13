import { bootstrap } from "./bootstrap.ts";

const { runtime, mode, telemetry } = await bootstrap({ workerOnly: true });
if (mode !== "distributed") throw new Error("Worker process requires EXECUTION_MODE=distributed");
runtime.startWorker();
console.info("Valida worker started");
const shutdown = async () => { await runtime.close(); await telemetry?.shutdown(); };
process.once("SIGINT", () => { void shutdown(); });
process.once("SIGTERM", () => { void shutdown(); });
