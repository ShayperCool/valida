import { Client } from "@langchain/langgraph-sdk";
import { createHash, randomUUID } from "node:crypto";
import { existsSync } from "node:fs";
import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { isDeepStrictEqual } from "node:util";
import { dirname, join, resolve } from "node:path";
import { availableParallelism, tmpdir, totalmem } from "node:os";
import type { Phase } from "./cgroup_sampler.ts";

type Trial = {
  wave: number | "warmup";
  thread_id: string;
  start_epoch_ms: number;
  end_epoch_ms: number;
  latency_ms: number;
  status: "completed" | "error";
  input_equal_reference: boolean;
  observed_result: unknown;
  observed_output: unknown;
  result_equal_expected: boolean;
  error?: string;
};

function option(name: string, fallback?: string): string {
  const prefix = `--${name}=`;
  const value = process.argv.slice(2).find(arg => arg.startsWith(prefix))?.slice(prefix.length);
  if (value !== undefined) return value;
  if (fallback !== undefined) return fallback;
  throw new Error(`Missing --${name}=...`);
}

function integer(name: string, fallback: number, minimum: number, maximum = Number.MAX_SAFE_INTEGER): number {
  const value = Number(option(name, String(fallback)));
  if (!Number.isInteger(value) || value < minimum || value > maximum) {
    throw new Error(`--${name} must be an integer from ${minimum} to ${maximum}`);
  }
  return value;
}

function atPath(value: unknown, path: string): unknown {
  return path.split(".").reduce<unknown>((current, key) =>
    current && typeof current === "object" ? (current as Record<string, unknown>)[key] : undefined, value);
}

function inputRecord(value: unknown): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("--input-json must be a JSON object");
  }
  return value as Record<string, unknown>;
}

function percentile(values: number[], fraction: number): number | null {
  if (!values.length) return null;
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.ceil(sorted.length * fraction) - 1]!;
}

function summarize(trials: Trial[], durationMs: number) {
  const completed = trials.filter(trial => trial.status === "completed");
  return { runs: trials.length, completed: completed.length, errors: trials.length - completed.length,
    wall_duration_ms: durationMs,
    latency_p50_ms: percentile(completed.map(trial => trial.latency_ms), 0.5),
    latency_p95_ms: percentile(completed.map(trial => trial.latency_ms), 0.95),
    throughput_successful_runs_per_second: durationMs > 0 ? completed.length / (durationMs / 1000) : null };
}

async function pool<T, R>(items: T[], concurrency: number, run: (item: T) => Promise<R>): Promise<R[]> {
  const results: R[] = new Array(items.length);
  let index = 0;
  await Promise.all(Array.from({ length: Math.min(concurrency, items.length) }, async () => {
    while (index < items.length) {
      const current = index++;
      results[current] = await run(items[current]!);
    }
  }));
  return results;
}

async function main() {
  const url = option("url", process.env.VALIDA_API_URL ?? "http://127.0.0.1:2026");
  const platform = option("platform", "valida");
  const assistant = option("assistant", "benchmark");
  const input = inputRecord(JSON.parse(option("input-json", '{"seed":7,"increment":5}')));
  const expected = JSON.parse(option("expected-json", "24")) as unknown;
  const resultPath = option("result-path", "result");
  const warmupRuns = integer("warmup-runs", 5, 1);
  const threads = integer("threads", 50, 1, 50);
  const waves = integer("waves", 10, 1);
  const concurrency = integer("concurrency", 50, 1, 50);
  const idleMs = integer("idle-ms", 15_000, 0);
  const timeoutMs = integer("run-timeout-ms", 120_000, 1);
  const periodMs = integer("resource-period-ms", 110, 100);
  const outFile = resolve(option("out", `benchmarks/results/${platform}-${new Date().toISOString().replaceAll(":", "-")}.json`));
  const resourceFile = resolve(option("resource-out", outFile.replace(/\.json$/, "") + ".resources.json"));
  const containers = process.argv.slice(2).filter(arg => arg.startsWith("--container="));
  const client = new Client({ apiUrl: url, apiKey: null });
  const canonicalInput = structuredClone(input);
  const inputSha256 = createHash("sha256").update(JSON.stringify(canonicalInput)).digest("hex");
  const report: {
    schema: string; platform: string; url: string; assistant: string; input: unknown; input_sha256: string;
    expected_result: unknown; result_path: string; configuration: Record<string, unknown>;
    host: { logical_cpus_available: number; total_memory_bytes: number };
    started_at: string; finished_at?: string; phases: Phase[]; trials: Trial[];
    setup_thread_ids: string[]; warmup?: ReturnType<typeof summarize>;
    load?: ReturnType<typeof summarize>; waves?: ReturnType<typeof summarize>[];
    resource_file?: string; resource_summary?: unknown; resource_error?: string; fatal_error?: string;
  } = { schema: "valida-benchmark/v1", platform, url, assistant, input: canonicalInput,
    input_sha256: inputSha256, expected_result: expected, result_path: resultPath,
    host: { logical_cpus_available: availableParallelism(), total_memory_bytes: totalmem() },
    configuration: { warmup_runs: warmupRuns, threads, waves, concurrency,
      idle_ms: idleMs, run_timeout_ms: timeoutMs, resource_period_ms: periodMs },
    started_at: new Date().toISOString(), phases: [], trials: [], setup_thread_ids: [] };
  await mkdir(dirname(outFile), { recursive: true });

  const scratch = join(tmpdir(), `valida-benchmark-${randomUUID()}`);
  const readyFile = join(scratch, "sampler.ready");
  const stopFile = join(scratch, "sampler.stop");
  let sampler: ReturnType<typeof Bun.spawn> | undefined;
  try {
    const response = await fetch(new URL("/health", url), { signal: AbortSignal.timeout(5_000) });
    if (!response.ok) throw new Error(`Health check failed: HTTP ${response.status}`);
    const assistants = await client.assistants.search();
    if (!assistants.some(candidate => candidate.assistant_id === assistant ||
      candidate.graph_id === assistant || candidate.name === assistant)) {
      throw new Error(`Assistant '${assistant}' is unavailable at ${url}`);
    }
    if (containers.length) {
      await mkdir(scratch, { recursive: true });
      sampler = Bun.spawn([process.execPath, new URL("./cgroup_sampler.ts", import.meta.url).pathname,
        ...containers, `--period-ms=${periodMs}`, `--ready-file=${readyFile}`, `--stop-file=${stopFile}`,
        `--workload-file=${outFile}`, `--out=${resourceFile}`], { stdout: "inherit", stderr: "inherit" });
      for (let attempt = 0; attempt < 100 && !existsSync(readyFile); attempt++) {
        if (sampler.exitCode !== null) throw new Error(`Resource sampler exited with ${sampler.exitCode}`);
        await Bun.sleep(50);
      }
      if (!existsSync(readyFile)) throw new Error("Resource sampler did not become ready in 5 seconds");
      report.resource_file = resourceFile;
    }

    const runTrial = async (threadId: string, wave: Trial["wave"]): Promise<Trial> => {
      const sentInput = structuredClone(canonicalInput);
      const inputEqual = isDeepStrictEqual(sentInput, canonicalInput);
      const started = performance.now();
      const startEpoch = Date.now();
      let output: unknown;
      let observed: unknown;
      let error: string | undefined;
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(new Error("Run timed out")), timeoutMs);
      try {
        output = await client.runs.wait(threadId, assistant, { input: sentInput, signal: controller.signal });
        observed = atPath(output, resultPath);
        if (!isDeepStrictEqual(observed, expected)) {
          error = `Expected ${JSON.stringify(expected)} at '${resultPath}', got ${JSON.stringify(observed)}`;
        }
      } catch (cause) {
        error = cause instanceof Error ? cause.message : String(cause);
      } finally {
        clearTimeout(timeout);
      }
      return { wave, thread_id: threadId, start_epoch_ms: startEpoch, end_epoch_ms: Date.now(),
        latency_ms: performance.now() - started, status: error ? "error" : "completed",
        input_equal_reference: inputEqual, observed_result: observed ?? null,
        observed_output: output ?? null, result_equal_expected: isDeepStrictEqual(observed, expected),
        ...(error ? { error } : {}) };
    };
    const phase = async <T>(kind: Phase["kind"], work: () => Promise<T>, wave?: number): Promise<T> => {
      const start = Date.now();
      try { return await work(); }
      finally { report.phases.push({ kind, ...(wave ? { wave } : {}),
        start_epoch_ms: start, end_epoch_ms: Date.now() }); }
    };

    const warmupThread = await client.threads.create();
    await phase("warmup", async () => {
      for (let index = 0; index < warmupRuns; index++) {
        report.trials.push(await runTrial(warmupThread.thread_id, "warmup"));
      }
    });
    const setupIds = await phase("setup", () => pool(Array.from({ length: threads }, (_, index) => index),
      concurrency, async () => (await client.threads.create()).thread_id));
    report.setup_thread_ids = setupIds;
    await phase("idle", () => Bun.sleep(idleMs));
    for (let wave = 1; wave <= waves; wave++) {
      const results = await phase("load", () => pool(setupIds, concurrency,
        threadId => runTrial(threadId, wave)), wave);
      report.trials.push(...results);
    }
  } catch (cause) {
    report.fatal_error = cause instanceof Error ? cause.message : String(cause);
    process.exitCode = 1;
  } finally {
    report.finished_at = new Date().toISOString();
    const warmup = report.phases.find(phase => phase.kind === "warmup");
    const loads = report.phases.filter(phase => phase.kind === "load");
    if (warmup) report.warmup = summarize(report.trials.filter(trial => trial.wave === "warmup"),
      warmup.end_epoch_ms - warmup.start_epoch_ms);
    report.waves = loads.map(phase => summarize(report.trials.filter(trial => trial.wave === phase.wave),
      phase.end_epoch_ms - phase.start_epoch_ms));
    if (loads.length) report.load = summarize(report.trials.filter(trial => trial.wave !== "warmup"),
      loads.at(-1)!.end_epoch_ms - loads[0]!.start_epoch_ms);
    if (report.trials.some(trial => trial.status === "error")) process.exitCode = 1;
    await writeFile(outFile, `${JSON.stringify(report, null, 2)}\n`);
    if (sampler) {
      await writeFile(stopFile, "stop\n");
      const code = await sampler.exited;
      if (code === 0) {
        const resources = JSON.parse(await readFile(resourceFile, "utf8")) as { summary: unknown };
        report.resource_summary = resources.summary;
      } else {
        report.resource_error = `Resource sampler exited with ${code}`;
        process.exitCode = 1;
      }
      await writeFile(outFile, `${JSON.stringify(report, null, 2)}\n`);
    }
    await rm(scratch, { recursive: true, force: true });
    console.log(JSON.stringify({ workload_file: outFile, resource_file: report.resource_file,
      load: report.load, fatal_error: report.fatal_error, resource_error: report.resource_error }));
  }
}

if (import.meta.main) main().catch(cause => { console.error(cause); process.exitCode = 1; });
