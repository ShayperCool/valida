import { Queue, Worker, type Job } from "bullmq";
import IORedis from "ioredis";

export interface QueueConfig { redisUrl: string; name?: string; concurrency?: number }

/** BullMQ keeps API instances and workers independent. Each run ID is a stable job ID. */
export class RunQueue {
  private readonly producer: IORedis;
  private consumer?: IORedis;
  private readonly queue: Queue<{ runId: string }>;
  private worker?: Worker<{ runId: string }>;
  private starting?: Promise<void>;
  readonly name: string;

  constructor(private readonly config: QueueConfig) {
    this.name = config.name ?? "valida-runs";
    // The API must return promptly when Redis is down; the run remains in PostgreSQL
    // and a worker's database poller will pick it up.
    this.producer = new IORedis(config.redisUrl, {
      maxRetriesPerRequest: 1, enableOfflineQueue: false, lazyConnect: true,
    });
    this.producer.on("error", () => {});
    this.queue = new Queue(this.name, { connection: this.producer });
    this.queue.on("error", () => {});
  }

  private async waitForRedis(): Promise<boolean> {
    if (this.producer.status === "ready") return true;
    if (this.producer.status === "wait") {
      try { await this.producer.connect(); }
      catch { return false; }
      try { return await this.producer.ping() === "PONG"; }
      catch { return false; }
    }
    // Queue may already be connecting when startWorker is called. Wait once for
    // that attempt, then leave execution to the database poller on failure.
    return new Promise(resolve => {
      const done = (ready: boolean) => {
        clearTimeout(timer);
        this.producer.off("ready", onReady);
        this.producer.off("error", onError);
        this.producer.off("end", onError);
        resolve(ready);
      };
      const onReady = () => done(true);
      const onError = () => done(false);
      const timer = setTimeout(() => done(this.producer.status === "ready"), 1_000);
      this.producer.on("ready", onReady);
      this.producer.on("error", onError);
      this.producer.on("end", onError);
      if (this.producer.status === "ready") done(true);
    });
  }

  async enqueue(runId: string): Promise<void> {
    if (!await this.waitForRedis()) throw new Error("Redis is unavailable");
    await this.queue.add("run", { runId }, {
      jobId: runId,
      attempts: 5,
      backoff: { type: "exponential", delay: 1_000 },
      removeOnComplete: { age: 86_400, count: 10_000 },
      removeOnFail: { age: 604_800, count: 10_000 },
    });
  }

  async start(process: (runId: string) => Promise<void>): Promise<void> {
    if (this.worker) return;
    if (this.starting) return this.starting;
    this.starting = (async () => {
      // Construct BullMQ's blocking connection only after Redis is reachable.
      // Otherwise its initialization can emit late errors after shutdown.
      if (!await this.waitForRedis()) return;
      await this.producer.ping();
      this.consumer = new IORedis(this.config.redisUrl, { maxRetriesPerRequest: null });
      this.consumer.on("error", () => {});
      this.worker = new Worker(this.name, async (job: Job<{ runId: string }>) => {
        await process(job.data.runId);
      }, { connection: this.consumer, concurrency: this.config.concurrency ?? 4 });
      this.worker.on("error", () => {});
    })().finally(() => { this.starting = undefined; });
    return this.starting;
  }

  async close(): Promise<void> {
    await this.starting?.catch(() => {});
    await this.worker?.close();
    await this.queue.close();
    this.producer.disconnect();
    this.consumer?.disconnect();
  }
}
