# Agent Protocol checkpoint benchmark

`workload.ts` uses the official `@langchain/langgraph-sdk` client against either Valida or Aegra. Both servers must expose an assistant named `benchmark` that accepts `{ "seed": 7, "increment": 5 }` and returns `{ "result": 24 }`. The script verifies the result for every run and saves the full observed output. It does not define or modify a graph.

The default full run warms up on a separate thread, creates 50 threads, waits 15 seconds to measure idle resources, then executes 10 burst waves of `runs.wait` on those same threads with at most 50 in-flight runs. Reusing each thread exercises checkpoint writes and reads. The workload saves every trial's latency, completion or error, input equality, observed output and result equality. It also reports p50/p95 successful latency, successful runs per second, and wall duration for each wave and the combined load.

The independent `cgroup_sampler.ts` process starts before warmup. It resolves each named Docker container to its host cgroup v2 files, samples `cpu.stat`, `memory.current`, and `memory.stat` about every 110 ms, and saves raw time series plus phase summaries. CPU percent uses one core as 100%; a stack using two full cores reports 200%. The summary includes app-only and whole-stack metrics (app, PostgreSQL, Redis): idle average CPU and median RAM, load peak CPU from intervals of at least 100 ms and peak RAM, sample counts and interval counts. Anonymous and file-backed memory are reported separately because PostgreSQL file cache can grow between sequential runs. A separate file retains every sample and per-container metrics.

The matching graphs are [`../examples/benchmark/graph.ts`](../examples/benchmark/graph.ts) and [`aegra/graph.py`](aegra/graph.py). Both have `add` and `double` nodes with a 200 ms async delay in each node. The Valida config is [`../examples/benchmark/valida.json`](../examples/benchmark/valida.json); Aegra's is [`aegra/aegra.json`](aegra/aegra.json). For the recorded comparison, Aegra was checked out at `bbc784646e6a3912cf05b6b9de0054fdf50a36b7` (aegra-api 0.10.5) and built with its official `deployments/docker/Dockerfile` and `uv.lock`. Valida was built from this repository with Bun 1.4.1.

Both applications used one API container with its worker inside, limited to 4 CPUs and 4 GiB of RAM and swap disabled. Aegra used `REDIS_BROKER_ENABLED=true`, `WORKER_COUNT=1`, `N_JOBS_PER_WORKER=50`, `AUTH_TYPE=noop`, and `CRON_ENABLED=false`. Valida used `EXECUTION_MODE=distributed`, `RUN_WORKER_IN_API=true`, and `execution.concurrency=50`. Telemetry exporters were disabled for both. They connected to the same `pgvector/pgvector:0.8.6-pg17-bookworm` PostgreSQL container (2 CPUs, 1 GiB) and `redis:7-alpine` container (1 CPU, 256 MiB), on separate databases and Redis DB indexes. Restart PostgreSQL and Redis between platform trials to reduce file-cache carryover, and keep only the app under test running. PostgreSQL `memory.current` still includes reclaimable file cache, so compare app memory and `memory.stat` anonymous memory first.

Run the platforms sequentially. For a comparison, repeat each platform three times in alternating order (the recorded order was A-V-V-A-A-V) while keeping its graph, input, in-flight limit and shared-service limits identical. A single run per platform on the recorded local ports looks like this:

```bash
bun benchmarks/workload.ts \
  --platform=valida --url=http://127.0.0.1:22028 \
  --container=app:valida-bench-app \
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
