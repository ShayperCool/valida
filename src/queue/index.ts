import { Queue, Worker, type Job } from "bullmq";
import IORedis from "ioredis";

export interface QueueConfig { redisUrl: string; name?: string; concurrency?: number }

/** BullMQ keeps API instances and workers independent. Each run ID is a stable job ID. */
export class RunQueue {
  private readonly connection: IORedis;
  private readonly queue: Queue<{ runId: string }>;
  private worker?: Worker<{ runId: string }>;
  readonly name: string;

  constructor(private readonly config: QueueConfig) {
    this.name = config.name ?? "valida-runs";
    this.connection = new IORedis(config.redisUrl, { maxRetriesPerRequest: null });
    this.queue = new Queue(this.name, { connection: this.connection });
  }

  async enqueue(runId: string): Promise<void> {
    await this.queue.add("run", { runId }, {
      jobId: runId,
      attempts: 5,
      backoff: { type: "exponential", delay: 1_000 },
      removeOnComplete: { age: 86_400, count: 10_000 },
      removeOnFail: { age: 604_800, count: 10_000 },
    });
  }

  start(process: (runId: string) => Promise<void>): Worker<{ runId: string }> {
    if (this.worker) return this.worker;
    this.worker = new Worker(this.name, async (job: Job<{ runId: string }>) => {
      await process(job.data.runId);
    }, { connection: this.connection, concurrency: this.config.concurrency ?? 4 });
    return this.worker;
  }

  async close(): Promise<void> {
    await this.worker?.close();
    await this.queue.close();
    await this.connection.quit();
  }
}
