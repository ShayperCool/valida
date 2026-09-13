# Agent Protocol checkpoint benchmark

`workload.ts` uses the official `@langchain/langgraph-sdk` client against either Valida or Aegra. Both servers must expose an assistant named `benchmark` that accepts `{ "seed": 7, "increment": 5 }` and returns `{ "result": 24 }`. The script verifies the result for every run and saves the full observed output. It does not define or modify a graph.

The default full run warms up on a separate thread, creates 50 threads, waits 15 seconds to measure idle resources, then executes 10 burst waves of `runs.wait` on those same threads with at most 50 in-flight runs. Reusing each thread exercises checkpoint writes and reads. The workload saves every trial's latency, completion or error, input equality, observed output and result equality. It also reports p50/p95 successful latency, successful runs per second, and wall duration for each wave and the combined load.

The independent `cgroup_sampler.ts` process starts before warmup. It resolves each named Docker container to its host cgroup v2 files, samples `cpu.stat`, `memory.current`, and `memory.stat` about every 110 ms, and saves raw time series plus phase summaries. CPU percent uses one core as 100%; a stack using two full cores reports 200%. The summary includes app-only and whole-stack metrics (app, PostgreSQL, Redis): idle average CPU and median RAM, load peak CPU from intervals of at least 100 ms and peak RAM, sample counts and interval counts. Anonymous and file-backed memory are reported separately because PostgreSQL file cache can grow between sequential runs. A separate file retains every sample and per-container metrics.

Run the platforms sequentially against the same PostgreSQL and Redis containers. Replace the Valida app container name and URL with the running setup. For a comparison, repeat each platform three times in alternating order (for example V-A-A-V-V-A) while keeping its graph, input, in-flight limit, app CPU/memory limits and shared services identical. A single run per platform looks like this:

```bash
bun benchmarks/workload.ts \
  --platform=valida --url=http://127.0.0.1:2026 \
  --container=app:valida-bench-api \
  --container=postgres:valida-bench-postgres \
  --container=redis:valida-bench-redis \
  --out=benchmarks/results/valida-1.json

bun benchmarks/workload.ts \
  --platform=aegra --url=http://127.0.0.1:22027 \
  --container=app:valida-bench-aegra \
  --container=postgres:valida-bench-postgres \
  --container=redis:valida-bench-redis \
  --out=benchmarks/results/aegra-1.json
```

Each command writes the workload JSON to `--out` and resource JSON beside it with `.resources.json` appended before the extension. `resource_summary` in the workload file mirrors the idle/load aggregate in the resource file. Container names must refer to running Docker containers on a Linux cgroup v2 host. Include every app or worker container with an additional `--container=app:<name>`; all named containers are counted in `stack`, while only app/worker containers are counted in `app`. Without `--container`, the workload still runs but has no resource report.

For a brief wiring check, use `--warmup-runs=1 --threads=2 --waves=1 --concurrency=2 --idle-ms=300`; this is not a performance measurement. Other options include `--assistant`, `--input-json`, `--result-path`, `--expected-json`, `--run-timeout-ms`, `--resource-period-ms`, and `--resource-out`. The full run defaults to the benchmark input and result above. Any failed or incorrect run appears as an error in its trial record and makes the process exit nonzero.
