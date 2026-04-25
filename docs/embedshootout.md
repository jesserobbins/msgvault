# embedshootout

Developer-side benchmark harness for the embed pipeline. Defines stratified frozen sample sets, runs sweeps over endpoint × model × batch_size × workers × preprocess settings, and persists per-cell metrics in `vectors.db` for cross-run comparison.

Spec: [`docs/specs/2026-04-25-embedshootout-harness-design.md`](specs/2026-04-25-embedshootout-harness-design.md).

## Build

```bash
make embedshootout
```

Produces `./embedshootout` at the repo root. Not built by `make build` / `make build-release` — it ships only as a developer tool, alongside `mimeshootout`. The binary is gitignored.

## Workflow

```bash
# Create a frozen sample of 2000 messages, stratified.
./embedshootout sample-create -name baseline -size 2000 \
    -stratify "length=short:25%,medium:50%,long:25%" -seed 42

# One-off run against a single endpoint.
./embedshootout run -mode endpoint -sample baseline \
    -endpoint http://localhost:1234/v1 -model nomic-embed-text \
    -dimension 768 -batch-size 32 -workers 1

# Sweep across endpoints, models, batch sizes (auto-writes the
# synthesized plan to ~/.msgvault/sweeps/auto/<UTC>.toml).
./embedshootout sweep -sample baseline \
    -endpoints "http://localhost:1234/v1,http://maclocal:8080/v1" \
    -models "nomic-embed-text,embeddinggemma-300m" \
    -dimensions "768,768" \
    -batch-sizes "16,32,64" -workers "1,2,4"

# Or run a saved plan TOML.
./embedshootout sweep -plan sweeps/2026-04-batch-size.toml

# List recent runs.
./embedshootout list

# Show one run's details.
./embedshootout show -run 42

# Compare two runs side-by-side; ≥5% deltas marked with *.
./embedshootout compare -runs 42,43

# JSON output for scripting.
./embedshootout list -json
./embedshootout show -run 42 -json
```

## Modes

- **endpoint** (default): preprocess + `Client.Embed`, vectors discarded. Measures the server. No DB writes outside of bench tables.
- **pipeline**: drives `embed.Worker.RunOnce` against a throwaway `bench:`-prefixed generation. Measures the full msgvault path. The generation is dropped on every return path. Pipeline cells run strictly sequentially because `state='building'` is exclusive.

## Stratification axes

`-stratify "axis=v1:wpct,v2:wpct,...;axis2=..."`. Recognized axes:

- `length` — `short` < 500 chars, `medium` 500–5000, `long` > 5000 (post-preprocess).
- `year` — UTC year from `messages.sent_at`.
- `account` — `sources.identifier`.
- `attachments` — `with` / `without`.
- `source_type` — `sources.kind`. Today only `email` has embedded messages; non-`email` strata error with "0 candidate messages" until the importer enqueue work lands.

## Storage

All bench state lives in `vectors.db`:

- `bench_samples` / `bench_sample_messages` — frozen sample sets with per-ID stratum tags.
- `bench_sweeps` / `bench_runs` — sweep + per-cell metrics. Per-batch detail is rolled up into the run's percentile + counter columns rather than stored per-row; the spec's `bench_batches` table is unimplemented (would be added when an actual consumer needs it).

Tables are created lazily on first write. Read verbs (`sample-list`, `list`, `show`, `compare`) error cleanly with "no bench data yet" if the schema is absent.

## Orphan recovery

Every write verb runs `bench.CleanupOrphanGenerations(ctx, db, bb, time.Hour)` at startup. Drops `bench:` generations older than one hour with no `bench_runs.status='running'` referencing them. Orphans only appear after a process kill mid-pipeline-run; the normal exit paths drop the gen via `RunPipeline`'s defer.

## Data flow at a glance

```
sample-create     → bench_samples, bench_sample_messages
run    (endpoint) → fetch + preprocess → RunEndpoint    → bench_runs
run    (pipeline) → seed pending → embed.Worker.RunOnce → bench_runs, gen dropped
sweep             → bench_sweeps + many bench_runs
list/show/compare → read-only queries against bench_runs
```

## Limits

- Pipeline-mode cells run sequentially (the unique partial index on `state='building'` allows one bench gen at a time). Endpoint cells also run sequentially in this implementation.
- Cross-machine result comparison is not supported (`config_hash` + `sample_name` are the dedup keys; machine identity is not recorded).
- No retrieval-quality metrics — this harness only measures speed. Quality eval is planned as a separate workstream.
