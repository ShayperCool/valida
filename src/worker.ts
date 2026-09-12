import { bootstrap } from "./bootstrap.ts";

const { runtime, mode } = await bootstrap({ workerOnly: true });
if (mode !== "distributed") throw new Error("Worker process requires EXECUTION_MODE=distributed");
runtime.startWorker();
console.info("Valida worker started");
const shutdown = async () => runtime.close();
process.once("SIGINT", () => { void shutdown(); });
process.once("SIGTERM", () => { void shutdown(); });
