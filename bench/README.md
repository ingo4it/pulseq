# bench

`load.ts` is the load generator behind `pnpm demo:load` and `pnpm bench`.

## What it does

1. Enqueues a burst of `demo.process` jobs with an **injected downstream
   failure rate** (`--fail-rate`) and a small **poison rate** (`--poison-rate`,
   jobs that always fail and therefore reach the DLQ). This is deliberate — the
   throughput and latency numbers are only interesting if the retry and DLQ
   paths are being exercised while they're measured.
2. Waits for the queue to drain (ready + delayed + in-flight quiet for ~3s).
3. Reads the settled job/attempt records from Postgres and reports throughput,
   latency p50/p95/p99, retry rate (`extra attempts / jobs`), and DLQ rate.

## Running

```bash
docker compose up -d
pnpm migrate
pnpm dev:worker          # in another shell — or several, to test scaling
pnpm bench --preset bench --out bench/results/latest.json
```

Presets: `demo` (500 jobs, watch the Grafana dashboard) and `bench` (20k jobs).
Override any knob: `--jobs`, `--fail-rate`, `--poison-rate`, `--namespace`.

## Results

`results/latest.json` ships as a `not-yet-measured` placeholder. Real numbers
go here (and into the README table) only after a run on real hardware —
nothing is fabricated.
