import { execFileSync } from "node:child_process";
import { existsSync } from "node:fs";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, resolve, sep } from "node:path";

export type ContainerRole = "app" | "postgres" | "redis";
export type ContainerSpec = { name: string; role: ContainerRole; id: string; cgroupPath: string };
export type ContainerSample = {
  cpu_usage_usec: number;
  memory_current_bytes: number;
  memory_anon_bytes: number;
  memory_file_bytes: number;
  cpu_percent_one_core: number | null;
};
export type Sample = {
  at_epoch_ms: number;
  elapsed_ms: number;
  interval_ms: number | null;
  containers: Record<string, ContainerSample>;
};
export type Phase = {
  kind: "warmup" | "setup" | "idle" | "load";
  wave?: number;
  start_epoch_ms: number;
  end_epoch_ms: number;
};

const cgroupRoot = "/sys/fs/cgroup";

export function parseCpuUsage(stat: string): number {
  const match = /^usage_usec\s+(\d+)$/m.exec(stat);
  if (!match) throw new Error("cgroup v2 cpu.stat has no usage_usec");
  return Number(match[1]);
}

export function parseMemoryStat(stat: string): { anon: number; file: number } {
  const anon = /^anon\s+(\d+)$/m.exec(stat);
  const file = /^file\s+(\d+)$/m.exec(stat);
  if (!anon || !file) throw new Error("cgroup v2 memory.stat has no anon or file counter");
  return { anon: Number(anon[1]), file: Number(file[1]) };
}

function option(name: string, fallback?: string): string {
  const prefix = `--${name}=`;
  const value = process.argv.slice(2).find(arg => arg.startsWith(prefix))?.slice(prefix.length);
  if (value !== undefined) return value;
  if (fallback !== undefined) return fallback;
  throw new Error(`Missing --${name}=...`);
}

function parseContainer(value: string): { role: ContainerRole; name: string } {
  const separator = value.indexOf(":");
  const role = value.slice(0, separator);
  const name = value.slice(separator + 1);
  if (!["app", "postgres", "redis"].includes(role) || !name) {
    throw new Error(`Invalid --container=${value}; use app:name, postgres:name, or redis:name`);
  }
  return { role: role as ContainerRole, name };
}

function inspectContainer(role: ContainerRole, name: string): ContainerSpec {
  const inspected = JSON.parse(execFileSync("docker", ["inspect", name], { encoding: "utf8" }));
  const container = inspected[0] as { Id: string; State: { Pid: number } };
  if (!container?.State?.Pid) throw new Error(`Docker container '${name}' is not running`);
  const cgroup = execFileSync("cat", [`/proc/${container.State.Pid}/cgroup`], { encoding: "utf8" });
  const relative = cgroup.split("\n").find(line => line.startsWith("0::"))?.slice(3);
  if (!relative) throw new Error(`Docker container '${name}' is not in cgroup v2`);
  const cgroupPath = resolve(cgroupRoot, `.${relative}`);
  if (cgroupPath !== cgroupRoot && !cgroupPath.startsWith(`${cgroupRoot}${sep}`)) {
    throw new Error(`Invalid cgroup path for '${name}'`);
  }
  if (!existsSync(`${cgroupPath}/cpu.stat`) || !existsSync(`${cgroupPath}/memory.current`) ||
    !existsSync(`${cgroupPath}/memory.stat`)) {
    throw new Error(`Missing cgroup v2 CPU or memory file for '${name}': ${cgroupPath}`);
  }
  return { name, role, id: container.Id, cgroupPath };
}

async function collect(containers: ContainerSpec[], started: number, previous: Sample | null): Promise<Sample> {
  const values = await Promise.all(containers.map(async container => {
    const [cpu, memory, memoryStat] = await Promise.all([
      readFile(`${container.cgroupPath}/cpu.stat`, "utf8"),
      readFile(`${container.cgroupPath}/memory.current`, "utf8"),
      readFile(`${container.cgroupPath}/memory.stat`, "utf8"),
    ]);
    const split = parseMemoryStat(memoryStat);
    return [container.name, { cpu_usage_usec: parseCpuUsage(cpu),
      memory_current_bytes: Number(memory.trim()), memory_anon_bytes: split.anon,
      memory_file_bytes: split.file, cpu_percent_one_core: null }] as const;
  }));
  const now = performance.now();
  const intervalMs = previous ? now - started - previous.elapsed_ms : null;
  const sample: Sample = { at_epoch_ms: Date.now(), elapsed_ms: now - started,
    interval_ms: intervalMs, containers: Object.fromEntries(values) };
  if (previous && intervalMs && intervalMs > 0) {
    for (const container of containers) {
      const current = sample.containers[container.name]!;
      const before = previous.containers[container.name]!;
      const used = current.cpu_usage_usec - before.cpu_usage_usec;
      current.cpu_percent_one_core = used >= 0 ? used / (intervalMs * 1000) * 100 : null;
    }
  }
  return sample;
}

function median(values: number[]): number | null {
  if (!values.length) return null;
  values.sort((a, b) => a - b);
  const middle = Math.floor(values.length / 2);
  return values.length % 2 ? values[middle]! : (values[middle - 1]! + values[middle]!) / 2;
}

export type ResourceSummary = {
  sample_count: number;
  cpu_interval_count: number;
  cpu_avg_percent_one_core: number | null;
  cpu_peak_percent_one_core_100ms: number | null;
  memory_median_bytes: number | null;
  memory_peak_bytes: number | null;
  memory_anon_median_bytes: number | null;
  memory_anon_peak_bytes: number | null;
  memory_file_median_bytes: number | null;
  memory_file_peak_bytes: number | null;
};

function summarizeGroup(samples: Sample[], phase: Phase, names: string[]): ResourceSummary {
  const inPhase = samples.filter(sample => sample.at_epoch_ms >= phase.start_epoch_ms &&
    sample.at_epoch_ms <= phase.end_epoch_ms);
  const intervals = inPhase.filter(sample => sample.interval_ms !== null &&
    sample.at_epoch_ms - sample.interval_ms >= phase.start_epoch_ms &&
    names.every(name => sample.containers[name]?.cpu_percent_one_core !== null));
  const cpuTime = intervals.reduce((sum, sample) => sum + sample.interval_ms!, 0);
  const cpuWeighted = intervals.reduce((sum, sample) => sum + names.reduce((used, name) =>
    used + sample.containers[name]!.cpu_percent_one_core!, 0) * sample.interval_ms!, 0);
  const peaks = intervals.filter(sample => sample.interval_ms! >= 100).map(sample =>
    names.reduce((used, name) => used + sample.containers[name]!.cpu_percent_one_core!, 0));
  const memories = inPhase.map(sample => names.reduce((bytes, name) =>
    bytes + sample.containers[name]!.memory_current_bytes, 0));
  const anon = inPhase.map(sample => names.reduce((bytes, name) =>
    bytes + sample.containers[name]!.memory_anon_bytes, 0));
  const file = inPhase.map(sample => names.reduce((bytes, name) =>
    bytes + sample.containers[name]!.memory_file_bytes, 0));
  return { sample_count: inPhase.length, cpu_interval_count: intervals.length,
    cpu_avg_percent_one_core: cpuTime > 0 ? cpuWeighted / cpuTime : null,
    cpu_peak_percent_one_core_100ms: peaks.length ? Math.max(...peaks) : null,
    memory_median_bytes: median(memories), memory_peak_bytes: memories.length ? Math.max(...memories) : null,
    memory_anon_median_bytes: median(anon), memory_anon_peak_bytes: anon.length ? Math.max(...anon) : null,
    memory_file_median_bytes: median(file), memory_file_peak_bytes: file.length ? Math.max(...file) : null };
}

export function summarizeResources(samples: Sample[], phases: Phase[], containers: ContainerSpec[]) {
  const summarize = (phase: Phase) => ({
    phase,
    app: summarizeGroup(samples, phase, containers.filter(container => container.role === "app").map(c => c.name)),
    stack: summarizeGroup(samples, phase, containers.map(container => container.name)),
    containers: Object.fromEntries(containers.map(container => [container.name,
      summarizeGroup(samples, phase, [container.name])])),
  });
  return phases.map(summarize);
}

async function main() {
  if (!existsSync(`${cgroupRoot}/cgroup.controllers`)) throw new Error("Host cgroup v2 is unavailable");
  const periodMs = Number(option("period-ms", "110"));
  if (!Number.isInteger(periodMs) || periodMs < 100) throw new Error("--period-ms must be >= 100");
  const specs = process.argv.slice(2).filter(arg => arg.startsWith("--container="))
    .map(arg => parseContainer(arg.slice("--container=".length)));
  if (!specs.some(spec => spec.role === "app")) throw new Error("At least one app container is required");
  if (new Set(specs.map(spec => spec.name)).size !== specs.length) throw new Error("Duplicate container name");
  const containers = specs.map(spec => inspectContainer(spec.role, spec.name));
  const readyFile = option("ready-file");
  const stopFile = option("stop-file");
  const workloadFile = option("workload-file");
  const outFile = option("out");
  await mkdir(dirname(outFile), { recursive: true });
  const started = performance.now();
  const samples: Sample[] = [];
  while (true) {
    samples.push(await collect(containers, started, samples.at(-1) ?? null));
    if (samples.length === 1) await writeFile(readyFile, "ready\n");
    if (existsSync(stopFile)) break;
    await Bun.sleep(periodMs);
  }
  const workload = JSON.parse(await readFile(workloadFile, "utf8")) as { phases: Phase[] };
  const idle = workload.phases.find(phase => phase.kind === "idle");
  const loads = workload.phases.filter(phase => phase.kind === "load");
  const aggregateLoad: Phase | undefined = loads.length ? { kind: "load",
    start_epoch_ms: loads[0]!.start_epoch_ms,
    end_epoch_ms: loads.at(-1)!.end_epoch_ms } : undefined;
  const report = { schema: "valida-cgroup-v2/v1", period_target_ms: periodMs,
    cpu_unit: "percent of one CPU core (100 = one fully used core)",
    memory_source: "memory.current, memory.stat anon/file", containers, samples,
    phase_summaries: summarizeResources(samples, workload.phases, containers),
    summary: {
      idle: idle ? summarizeResources(samples, [idle], containers)[0] : null,
      load: aggregateLoad ? summarizeResources(samples, [aggregateLoad], containers)[0] : null,
    } };
  await writeFile(outFile, `${JSON.stringify(report, null, 2)}\n`);
}

if (import.meta.main) main().catch(error => { console.error(error); process.exitCode = 1; });
