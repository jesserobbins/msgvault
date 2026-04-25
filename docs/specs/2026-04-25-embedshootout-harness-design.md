# `embedshootout`: stratified-sample sweep harness for embedding speed

Date: 2026-04-25
Status: Draft (pending review)
Scope: new `scripts/embedshootout/` binary, new library package `internal/vector/embed/bench/`, new tables in `vectors.db`, new optional `vector.BenchBackend` capability interface (`CreateBenchGeneration`, `DropGeneration`) — production `vector.Backend` is unchanged.

## Motivation

Tuning the embed pipeline against local OpenAI-compatible servers (LM Studio, ANE-on-maclocal-api, etc.) is currently guesswork. The progress line in `msgvault build-embeddings` reports throughput for one config in flight — there's no way to compare two endpoints, two models, two batch sizes, or two preprocessing settings without serially rebuilding the index, and no durable record of what was measured. The in-flight windowed-ETA spec helps the operator *see* current throughput; it doesn't help anyone *compare* configs or *reproduce* a measurement weeks later.

This spec adds an experimental harness, modeled on `mimeshootout`, that:

1. Defines **stratified, frozen sample sets** of message IDs (durable in `vectors.db`).
2. Runs **sweeps** across endpoint × model × batch_size × workers × preprocess settings.
3. Persists per-cell metrics (throughput, p50/p95 batch latency, error counts, truncation rate) in a results schema keyed for cross-run comparison.
4. Operates in two modes: **endpoint** (just `Client.Embed`) and **pipeline** (full `embed.Worker.RunOnce` against a throwaway generation).
5. Ships as a developer-side script — not part of the user-facing `msgvault` binary — so it can evolve quickly without changing the release surface.

The harness is the first of four planned embedding experiments (multi-server dispatch, multi-model concurrent indexing, this harness, retrieval-quality eval). The other three benefit from a measurement substrate that already exists; this spec builds it.

## Scope

In scope:

- New `scripts/embedshootout/main.go` (stdlib `flag`, verb-style first arg).
- New library `internal/vector/embed/bench/` (sample, runners, plan parsing, store, metrics, report).
- New tables in `vectors.db`: `bench_samples`, `bench_sample_messages`, `bench_sweeps`, `bench_runs`, `bench_batches`. All gated behind running the script at least once — msgvault startup does not create them.
- A new optional capability interface `vector.BenchBackend` (alongside the existing `FusingBackend` pattern) that the bench harness obtains via type assertion. It adds two methods — `CreateBenchGeneration` and `DropGeneration` — both restricted by contract to fingerprints prefixed `bench:`. The production `vector.Backend` interface is **not** modified; production code paths (sync, build-embeddings, hybrid search) cannot see these methods.
- Two new Makefile targets: `embedshootout`, `run-embedshootout`. `clean` updated.

Out of scope:

- Wiring iMessage / WhatsApp / .emlx importers into the embed enqueue pipeline. Today only Gmail sync (and the encoding-repair path) calls `Enqueuer.EnqueueMessages`, so the corpus available for benching is Gmail-only. Stratification by `source_type` is included in the schema/CLI but is effectively a no-op until that wiring lands as a separate effort (call it project E).
- Multi-endpoint dispatch within a single run cell. Cells measure one endpoint at a time. Fan-out across endpoints is project A (a separate spec).
- Concurrent multi-generation indexing. Project B (separate spec).
- Retrieval-quality metrics (recall@k, MRR, NDCG). Project D (separate spec). This harness only measures speed.
- Auto-tuning ("find optimal batch size for endpoint X"). Operator reads sweep table and decides.
- Cross-machine results comparison. `config_hash + sample_name` is enough for same-machine reproducibility; cross-machine adds noise we don't yet need to reason about.
- Live charts / TUI / HTML reports. Terminal tables + `--json` output cover scripting and inspection.

## CLI surface

The harness is one binary (`./embedshootout` after `make embedshootout`) with verb-style dispatch. Each verb owns its own `flag.FlagSet` so `-h` shows verb-specific help. DB paths default to `~/.msgvault/vectors.db` and `~/.msgvault/msgvault.db`, both overridable via `-vectors-db` and `-main-db`. `MSGVAULT_HOME` is honored.

```
./embedshootout sample-create -name X -size N [-stratify ...] [-seed S] [-notes ...]
./embedshootout sample-list
./embedshootout sample-show -name X
./embedshootout sample-delete -name X

./embedshootout run -mode endpoint -sample X \
    -endpoint URL -model M -dimension N -batch-size N -workers N \
    [-max-input-chars N] [-warmup-batches N] [-strip-quotes=bool] [-strip-signatures=bool] \
    [-timeout DUR] [-max-retries N] [-notes ...]

./embedshootout sweep -plan FILE
./embedshootout sweep -sample X -mode endpoint \
    -endpoints A,B -models X,Y -batch-sizes 16,32,64 -workers 1,2,4 \
    [-fixed-max-input-chars N] [-fixed-timeout DUR] [-notes ...]
    # auto-writes plan TOML to ~/.msgvault/sweeps/auto/<UTC ts>.toml

./embedshootout list [-sweep ID] [-json]
./embedshootout show -run RUN_ID [-json] [-batches]
./embedshootout compare -runs A,B,C [-json]
./embedshootout delete -run ID | -sweep ID | -before YYYY-MM-DD
```

`run` is a single-cell convenience. `sweep` is the matrix entrypoint. `compare` accepts 2+ run IDs and prints a side-by-side table.

## Plan TOML

```toml
notes = "compare nomic vs gemma at varying batch sizes"
sample = "sample-2k"
mode = "endpoint"
warmup_batches = 1

[fixed]
max_input_chars = 32768
timeout = "30s"
max_retries = 3
preprocess.strip_quotes = true
preprocess.strip_signatures = true

[[matrix]]
name = "endpoint"
values = ["http://localhost:1234/v1/embeddings", "http://maclocal:8080/v1/embeddings"]

[[matrix]]
name = "model"
values = ["nomic-embed-text", "embeddinggemma-300m"]

[[matrix]]
name = "dimension"
values = [768, 768]   # parallel to model; see "matrix axes" below

[[matrix]]
name = "batch_size"
values = [16, 32, 64, 128]

[[matrix]]
name = "workers"
values = [1, 2, 4]
```

Cells = cartesian product of `[[matrix]]` entries. The full plan TOML is **stored inline** in `bench_sweeps.plan_toml` so a sweep is reproducible even if the plan file is later deleted, edited, or never existed (the `sweep -sample ... -models ...` path writes its synthesized plan to the same column).

### Matrix axes and the `model`/`dimension` coupling

Most axes are independent (cartesian). The exception is `model` and `dimension`: when `model` and `dimension` appear together they are zipped position-wise rather than crossed, so `model[0]` pairs with `dimension[0]`, etc. This avoids generating nonsense cells like (`nomic-embed-text`, `1536`). If `dimension` is omitted, each model's dimension is resolved by querying the endpoint's first response and recording it.

Plans declare zipped axes by giving them the same length and the harness rejects mismatched lengths with a clear error. No syntactic flag is needed: `model` + `dimension` is the only zipped pair we expect; the test ("zipped axes have equal length and are consumed in lockstep") fails fast.

## Schema (vectors.db)

```sql
CREATE TABLE bench_samples (
    name           TEXT PRIMARY KEY,
    created_at     INTEGER NOT NULL,
    size           INTEGER NOT NULL,
    stratify_spec  TEXT,                  -- canonical JSON of stratification spec; NULL for unstratified
    seed           INTEGER,
    notes          TEXT
);

CREATE TABLE bench_sample_messages (
    sample_name    TEXT NOT NULL REFERENCES bench_samples(name) ON DELETE CASCADE,
    message_id     INTEGER NOT NULL,
    stratum        TEXT,                  -- e.g. 'length=long,year=2014,source=email'
    PRIMARY KEY (sample_name, message_id)
);

CREATE TABLE bench_sweeps (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at     INTEGER NOT NULL,
    finished_at    INTEGER,
    status         TEXT NOT NULL,          -- 'running' | 'completed' | 'aborted'
    plan_path      TEXT,                   -- file path if -plan was used; otherwise auto path
    plan_toml      TEXT NOT NULL,          -- inline copy of the plan
    git_sha        TEXT,                   -- best-effort short SHA when launched from a git checkout
    notes          TEXT
);

CREATE TABLE bench_runs (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    sweep_id       INTEGER REFERENCES bench_sweeps(id) ON DELETE CASCADE,
    started_at     INTEGER NOT NULL,
    finished_at    INTEGER,
    status         TEXT NOT NULL,          -- 'running' | 'completed' | 'aborted' | 'error'
    error_message  TEXT,
    mode           TEXT NOT NULL,          -- 'endpoint' | 'pipeline'
    sample_name    TEXT NOT NULL,
    config_json    TEXT NOT NULL,          -- resolved EmbeddingsConfig + Preprocess (canonical)
    config_hash    TEXT NOT NULL,          -- sha256 of config_json
    cell_json      TEXT NOT NULL,          -- e.g. {"endpoint":"...","model":"...","batch_size":32,"workers":2}
    generation_id  INTEGER,                -- pipeline mode only; populated when the throwaway gen is created and used for orphan cleanup; NULL in endpoint mode
    -- aggregate metrics (warmup batches excluded):
    msgs_total      INTEGER,
    msgs_succeeded  INTEGER,
    msgs_truncated  INTEGER,
    msgs_dropped    INTEGER,
    chars_total     INTEGER,
    elapsed_ms      INTEGER,                -- wall time of the run, warmup INCLUDED (so live progress matches)
    embed_ms_sum    INTEGER,                -- sum of Client.Embed call times, warmup excluded
    msg_per_sec     REAL,                   -- msgs_succeeded / (elapsed_ms_excl_warmup / 1000)
    us_per_char     REAL,
    ms_per_msg      REAL,
    batch_p50_ms    REAL,
    batch_p95_ms    REAL,
    batch_p99_ms    REAL,
    batch_max_ms    REAL,
    errors_4xx      INTEGER,
    errors_5xx      INTEGER,
    errors_429      INTEGER,
    errors_network  INTEGER,
    retries         INTEGER,                -- count of retry-attempt embeds (Client-side counter)
    -- pipeline-only (NULL in endpoint mode):
    claim_ms_sum    INTEGER,
    upsert_ms_sum   INTEGER,
    complete_ms_sum INTEGER
);
CREATE INDEX bench_runs_sweep_idx        ON bench_runs(sweep_id);
CREATE INDEX bench_runs_config_hash_idx  ON bench_runs(config_hash);

CREATE TABLE bench_batches (
    run_id      INTEGER NOT NULL REFERENCES bench_runs(id) ON DELETE CASCADE,
    seq         INTEGER NOT NULL,           -- 0-based across the whole run
    worker_id   INTEGER NOT NULL,
    msgs        INTEGER NOT NULL,
    chars       INTEGER NOT NULL,
    elapsed_ms  INTEGER NOT NULL,           -- end-to-end for the batch (preprocess+embed, or full pipeline in pipeline mode)
    embed_ms    INTEGER NOT NULL,           -- just the Client.Embed call portion
    truncated   INTEGER NOT NULL,
    error       TEXT,                       -- non-NULL means this batch failed; categorical class is recorded in bench_runs aggregates
    is_warmup   INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (run_id, seq, worker_id)
);
```

Schema is created lazily by `bench/store.go` on the first invocation of any verb that writes — `sample-create`, `run`, `sweep`. Read-only verbs (`sample-list`, `list`, `show`, `compare`) do not create the schema; they error with a clear "no bench data yet" message if the tables are absent. This keeps a fresh `vectors.db` clean of bench tables until a user actually opts in.

## Endpoint-mode runner (default path)

Per cell:

1. Load `bench_sample_messages` for the sample → list of message IDs.
2. Read subjects/bodies from main DB read-only with the same query as `embed.Worker.embedBatch` (subject + body_text + body_html with HTML fallback via `mime.StripHTML`).
3. Run `Preprocess` per message with the cell's resolved `PreprocessConfig` and `MaxInputChars`. Empty results count toward `msgs_dropped`; they do not enter the queue.
4. Push preprocessed `(id, text, chars)` records onto a buffered channel.
5. Spin up `workers` goroutines. Each worker:
   - Pulls items off the channel until it has `batch_size` (or channel closes).
   - First `warmup_batches` batches *per worker* are still embedded but flagged `is_warmup=1` and excluded from aggregate metrics.
   - Times each batch (`time.Now()` around `Client.Embed`); stores `embed_ms` separately from `elapsed_ms` (in endpoint mode they're nearly identical, but pipeline mode needs them split).
   - Discards returned vectors.
   - Writes a `bench_batches` row.
   - On error: writes a row with `error` populated and the error class accumulated into the run's aggregate counters (`errors_4xx`, `errors_5xx`, `errors_429`, `errors_network`). Errors do not abort the run; they are recorded and the worker continues.
6. After all workers finish, compute aggregates (`bench/metrics.go`) over non-warmup, non-error batches and update `bench_runs`.

`workers` is "N concurrent in-flight requests to one endpoint" — *not* fan-out across endpoints. Project A handles fan-out.

### Concurrency partitioning

Sample IDs flow through one buffered channel of capacity `2 * workers * batch_size`. Workers consume; each forms its own batches. No double-claim concern (no DB row lock to fight over — the queue is a Go channel, not `pending_embeddings`). A `sync.WaitGroup` + `errgroup` terminates the run.

## Pipeline-mode runner

Per cell:

1. Type-assert the configured `Backend` to `vector.BenchBackend`; if the assertion fails, `run -mode pipeline` errors with a clear "this backend does not support pipeline-mode benchmarking" message. (Today only `sqlitevec.Backend` exists, and it implements `BenchBackend`; future backends opt in.)
2. Allocate a throwaway generation by calling `BenchBackend.CreateBenchGeneration(ctx, model, dimension, runScope)` where `runScope` is `"<sweep_id>:<run_id>"`. The implementation forms the full fingerprint as `bench:<runScope>:<model>:<dimension>` and inserts the row in `building` state. **Crucially, `CreateBenchGeneration` does not perform the production seed pass that `CreateGeneration` does** — it leaves `pending_embeddings` empty for the new gen, and stamps `seeded_at` to a sentinel marker (`'bench-skipped'`) so the resume-path `EnsureSeeded` would see the gen as already seeded and skip it. This is essential: the production seed inserts *every embeddable message in the corpus*, which would obliterate the sample-bounded scope of a bench run.
3. Insert the sample's message IDs into `pending_embeddings` for that gen via direct `INSERT OR IGNORE`. The bench runner deliberately does **not** call `Enqueuer.EnqueueMessages` (which would enqueue for all non-retired generations, including the user's real one) and does **not** call `EnsureSeeded` (already a no-op on bench gens per the previous step, but worth being explicit so a future change to seed semantics doesn't accidentally pollute the bench gen).
4. Construct N `embed.Worker` instances sharing one `Backend`, one `MainDB`, and the throwaway gen. Each Worker's `Progress` callback writes a `bench_batches` row instead of (or in addition to) printing.
5. Run them concurrently against the same generation. The existing `claim_token` model already handles N workers on one queue — no race; this is exactly what project A will eventually rely on.
6. On completion (success or `ctx.Err()`), call `BenchBackend.DropGeneration(gen)` which:
   - Verifies fingerprint starts with `bench:`. If not, returns an error and aborts (defense in depth: callers must already check).
   - Deletes vector rows for the gen.
   - Deletes `pending_embeddings` rows for the gen.
   - Deletes the `index_generations` row.
7. Pipeline mode also records `claim_ms`, `upsert_ms`, `complete_ms` per batch. To deliver this, `embed.ProgressReport` gains three new optional fields (`ClaimElapsed`, `UpsertElapsed`, `CompleteElapsed time.Duration`); they default to zero, so the existing `build-embeddings` printer that ignores them is unaffected. The in-flight `2026-04-25-embed-windowed-eta-and-batch-downshift-design.md` spec leaves `ProgressReport`'s field set unchanged (smoothing lives in the printer), so this addition merges cleanly with that spec in either order. The bench-mode Progress callback sums these new fields into `bench_runs.{claim,upsert,complete}_ms_sum`.

If pipeline mode is interrupted mid-run (ctx cancellation, process kill), the throwaway generation and its `pending_embeddings` rows remain in `vectors.db`. They are recoverable two ways: (a) the next `embedshootout` invocation runs an opportunistic cleanup that calls `DropGeneration` for any `bench:`-prefixed generation whose `index_generations.started_at` is older than 1 hour AND whose `id` is not referenced by any `bench_runs` row with `status='running'`; (b) operators can manually `embedshootout delete -sweep ID` for a specific sweep, which cascade-deletes runs and triggers `DropGeneration` for each. The 1-hour threshold is conservative and well above the existing `ReclaimStale` derived stale window.

The opportunistic-cleanup query:

```sql
SELECT g.id
  FROM index_generations g
 WHERE g.fingerprint LIKE 'bench:%'
   AND g.started_at < strftime('%s','now') - 3600
   AND NOT EXISTS (
       SELECT 1 FROM bench_runs r
        WHERE r.generation_id = g.id
          AND r.status = 'running');
```

Each returned id is passed to `BenchBackend.DropGeneration`.

## Stratified sample creation

`-stratify` accepts a single `dim=values` clause. Values are either explicit weights (`length=short:25%,medium:50%,long:25%`) or implicit equal weighting (`year=2014,2015,2016`). Multi-axis stratification is not implemented; specs with more than one axis are rejected.

Supported dimensions:

- `length`: `short` < 500 chars, `medium` 500–5000, `long` > 5000. Measured **post-preprocess** (after `Preprocess(subject, body, max_input_chars, default-preprocess-cfg)`), since that's what the embedder actually sees.
- `year`: from `messages.internal_date` UTC year.
- `account`: from `messages.source_id → sources.identifier`. Today only `email`-kind sources have embedded messages, so meaningful `account=` values are Gmail addresses. Non-`email` sources exist in the `sources` table (WhatsApp / iMessage imports populate them), but their messages are not currently in the embedding corpus, so an `account` value resolving to a non-`email` source produces an empty stratum and the same error as `source_type` below.
- `attachments`: `with` (≥1 row in `attachments` for that message) / `without`.
- `source_type`: from `sources.kind`. Today the only `kind` whose messages are embedded is `email`; the dimension is accepted so the harness is ready when project E lands. Until then, requesting `source_type=email,imessage,whatsapp` against the current corpus produces a sample whose `imessage`/`whatsapp` strata are empty, and the sampler errors with `"stratum 'source_type=imessage' has 0 candidate messages — these source types are not currently in the embedding corpus"`. No silent degradation.

Sampling algorithm: stratum candidates are selected with the configured `--seed` (deterministic across machines for the same DB snapshot). Each stratum gets `floor(weight * size)` IDs; remainders are distributed by descending fractional part. The selected `(message_id, stratum)` pairs land in `bench_sample_messages`. Stratum is recorded so per-stratum rollups are possible later (`embedshootout show -run ... -by-stratum`, deferred).

The `length` bucket requires reading bodies, so sample creation does one full pass over candidate messages with the same fetch+preprocess as `embedBatch`. For 20+-year corpora this is slow once; samples are durable, so it's a one-time cost per sample. A `-progress` flag prints a progress bar during sample creation.

## Reports

### `show -run RUN_ID`

```
Run 42 (sweep 7, completed) — 2026-04-25 14:22:13 UTC
  mode=endpoint sample=sample-2k size=2000 stratify=length
  cell: endpoint=http://localhost:1234/v1/embeddings model=nomic-embed-text batch_size=64 workers=2
  config_hash: 7f3a…b91c
  msgs: 2000 ok, 0 dropped, 12 truncated
  throughput: 412 msg/s, 2.4 ms/msg, 1.83 µs/char
  batch latency: p50=148ms p95=284ms p99=412ms max=611ms
  errors: 0 4xx, 0 5xx, 0 429, 0 net   retries: 0
  duration: 4.85s (excl. warmup) / 5.21s (incl. warmup)
```

`-batches` adds a per-batch table sorted by `seq`. `-json` emits the row(s) as JSON.

### `compare -runs A,B,C`

Side-by-side table. Columns are runs, rows are metrics. Differences ≥ 5% relative to the leftmost column are highlighted (terminal: bold + arrow). `-json` emits the runs as a JSON array.

### `list [-sweep ID]`

Compact one-line-per-run listing sorted by `started_at DESC`. With `-sweep`, scoped to that sweep and ranked by `msg_per_sec DESC` to make the winner obvious.

### Sweep completion

After a sweep finishes, the script prints the same `list -sweep` ranked table by default. Errors (status='error') are surfaced at the top with their `error_message`.

## Reuse with the in-flight windowed-ETA spec

Both this spec and `2026-04-25-embed-windowed-eta-and-batch-downshift-design.md` need a `rateWindow` helper for live progress smoothing. The two specs are otherwise orthogonal: the windowed-ETA spec confines all of its smoothing to the printer and **does not modify `embed.ProgressReport`'s field set**, so the three new optional fields added by this spec (`ClaimElapsed`, `UpsertElapsed`, `CompleteElapsed`) merge cleanly with that spec in either order.

For the shared `rateWindow` helper, the implementation plan must pick one canonical location before either spec lands so both don't ship their own copy. Recommended location: `internal/vector/embed/progress/window.go` (a tiny shared package). Whichever spec ships first creates the package; the other imports it. The decision belongs to the implementation plan, not this spec.

## Code layout

```
scripts/embedshootout/
    main.go                    # stdlib `flag` dispatcher; mirrors mimeshootout style; thin shim over `bench` package

internal/vector/embed/bench/
    sample.go                  # stratified sampler; sample CRUD
    runner_endpoint.go         # endpoint-mode runner with worker pool
    runner_pipeline.go         # pipeline-mode runner over throwaway generation
    plan.go                    # TOML plan parse + matrix expansion + cell iterator (handles model/dimension zip)
    store.go                   # lazy DDL + CRUD against vectors.db
    metrics.go                 # aggregation from bench_batches; percentile helpers (p50/p95/p99/max)
    report.go                  # show/compare/list rendering (terminal + JSON)
    canonical.go               # canonical-JSON helpers for config_json / config_hash and stratify_spec storage
    bench_test.go

internal/vector/sqlitevec/
    drop_generation.go         # Backend.DropGeneration impl, refuses non-`bench:` fingerprints
```

Library lives under `internal/vector/embed/bench/` (not under `scripts/`) because it has substantial logic — stratified sampler, plan/matrix expansion, percentile aggregation, schema migration, two runner modes, comparison reports — and benefits from `go test ./...`. `mimeshootout` keeps everything in `main.go` because it's small; embedshootout is too big for that to remain healthy. The script itself stays thin: parse flags, dispatch to a library function, print.

## Backend interface change: `BenchBackend` capability

The production `Backend` interface (`internal/vector/backend.go`) is **not** modified. Instead, a new optional capability interface is added alongside `FusingBackend`:

```go
// In internal/vector/backend.go:

// BenchBackend is an optional capability for benchmark generation
// lifecycle: creating throwaway generations isolated from production
// activate/retire flow, and dropping them safely. The bench harness
// type-asserts a Backend to BenchBackend at run time; production code
// (sync, build-embeddings, hybrid search) does not see these methods
// and cannot accidentally call them.
type BenchBackend interface {
    Backend

    // CreateBenchGeneration creates a generation in `building` state
    // whose fingerprint is forced to begin with "bench:" — concretely
    // "bench:<runScope>:<model>:<dimension>" — so it is unambiguously
    // distinguishable from any production generation. Implementations
    // MUST skip the production seed pass that CreateGeneration performs
    // (the bench caller seeds pending_embeddings explicitly with a
    // sample-bounded set), and MUST stamp seeded_at to a sentinel value
    // ('bench-skipped' is recommended) so EnsureSeeded resume-paths
    // treat the gen as already seeded.
    CreateBenchGeneration(ctx context.Context, model string, dimension int, runScope string) (GenerationID, error)

    // DropGeneration removes a generation and its associated rows
    // (vectors, pending_embeddings, index_generations row). Implementations
    // MUST verify the fingerprint begins with "bench:" and return an
    // error otherwise — this is a hard contractual safety on top of any
    // caller-side check. This is a benchmark-only operation; production
    // index lifecycle uses RetireGeneration.
    DropGeneration(ctx context.Context, gen GenerationID) error
}
```

`sqlitevec.Backend` implements both methods.

`CreateBenchGeneration`:

1. Reject empty `runScope` (avoids fingerprint collision when sweep_id and run_id are both 0 in tests).
2. In one transaction:
   - Insert into `index_generations` with `state='building'`, `fingerprint='bench:<runScope>:<model>:<dimension>'`, `seeded_at='bench-skipped'`.
3. Commit and return the new `GenerationID`.

`DropGeneration`:

1. `SELECT fingerprint FROM index_generations WHERE id = ?` — error if no row, or fingerprint does not start with `bench:`.
2. In one transaction:
   - `DELETE FROM embeddings_v1 WHERE generation_id = ?` (or whichever vectors table the backend uses).
   - `DELETE FROM pending_embeddings WHERE generation_id = ?`.
   - `DELETE FROM index_generations WHERE id = ?`.
3. Commit.

The bench package wraps both with defense-in-depth fingerprint-prefix checks in `runner_pipeline.go` so a future bench bug constructing a non-`bench:` `GenerationID` cannot slip past.

### Why a separate interface, not new methods on `Backend`

`FusingBackend` is the existing precedent for backend-optional capabilities (hybrid-search FusedSearch). Following that pattern keeps three things clean: (a) the production interface stays minimal — sync, build-embeddings, hybrid have no reason to know about bench gens; (b) future backends can be added without implementing bench methods immediately; (c) the type-assertion failure path in `runner_pipeline.go` produces a clear, well-located error message instead of every backend having to stub out a "not supported" return.

## Makefile changes

```makefile
.PHONY: ... embedshootout run-embedshootout

# Build the embedding shootout tool
embedshootout:
	CGO_ENABLED=1 go build -tags "$(BUILD_TAGS)" -o embedshootout ./scripts/embedshootout

# Convenience smoke pass: single cell, default endpoint, requires a sample named 'default'
run-embedshootout: embedshootout
	./embedshootout run -mode endpoint -sample default -batch-size 32 -workers 1
```

`clean` updated:

```makefile
clean:
	rm -f msgvault msgvault.exe mimeshootout embedshootout
	rm -rf bin/
```

`help` adds the two new targets.

`embedshootout` is **not** built by `build` / `build-release`; it does not ship in releases. It's a developer-side tool that lives alongside the source.

## Testing

Library tests in `internal/vector/embed/bench/bench_test.go` and adjacent files. The script's `main.go` is intentionally thin and is exercised via a subprocess smoke test (one verb path).

### `sample`

- Stratification math: `length=short:25%,medium:50%,long:25%` against a synthetic 1000-row corpus produces 250/500/250 with the rounding-remainder rule and identical results across runs given the same seed.
- Empty stratum: `source_type=email,imessage` against an email-only corpus errors with the expected message and writes nothing.
- Length bucketing: a row with body that preprocesses to `< 500 chars` lands in `short` even when the raw body exceeds 500 chars (post-preprocess measurement).
- Idempotent recreate: `sample-create -name X` twice errors on the second call ("sample exists; use sample-delete first") rather than silently overwriting.
- Cascade: `sample-delete -name X` removes `bench_sample_messages` rows.

### `plan`

- Cartesian expansion: 2 endpoints × 3 batch sizes × 2 workers = 12 cells, in declared axis order.
- `model`/`dimension` zip: equal-length lists pair position-wise; unequal lengths error.
- Empty matrix axis: rejected with a useful error.
- Auto-write of synthesized plan from `sweep -sample ...` flags lands at `~/.msgvault/sweeps/auto/<UTC>.toml` and is identical to what gets stored in `bench_sweeps.plan_toml`.
- Plan TOML round-trip: parse → serialize → parse produces the same plan struct.

### `runner_endpoint`

Tests use a fake `EmbeddingClient` that records call timings:

- Warmup exclusion: `warmup_batches=1`, `workers=2`, `batch_size=10` against 50 messages — first batch per worker has `is_warmup=1`, aggregates compute over the rest.
- Worker concurrency: 4 workers + a fake client with 100ms sleep per call → wall time < 50% of sequential baseline (rough check that goroutines are actually concurrent).
- Error classification: client returns 4xx, 5xx, 429, network errors → counters increment correctly; run does not abort.
- Aggregate math: known per-batch timings → `msg_per_sec`, `us_per_char`, `ms_per_msg` match hand-calculated values.
- Percentile calculation: known batch-elapsed distribution (e.g. 100 samples uniform 50–150ms) → `batch_p50_ms`, `batch_p95_ms`, `batch_p99_ms` within 1ms of expected.
- All-empty corpus (every message preprocesses to empty): run completes with `msgs_dropped == size`, no batches recorded, aggregates safe (no NaN from zero-divides).

### `runner_pipeline`

Built on `internal/vector/embed/`'s existing fake-worker harness:

- Type-assertion failure: a backend that does not implement `BenchBackend` produces a clear "this backend does not support pipeline-mode benchmarking" error from `run -mode pipeline`. Test with a tiny stub backend that implements only `Backend`.
- Throwaway generation lifecycle: created via `CreateBenchGeneration` with `bench:` prefix, dropped via `DropGeneration` on success.
- `CreateBenchGeneration` does not seed `pending_embeddings`: after creation but before the runner's explicit `INSERT`s, the gen has zero pending rows. `EnsureSeeded` against the gen is a no-op (because `seeded_at` is set).
- `bench_runs.generation_id` is populated for pipeline runs and NULL for endpoint runs.
- Cancellation: `ctx.Cancel()` mid-run → throwaway generation remains until the next bench invocation's opportunistic cleanup, which then drops it. Verify the cleanup query (started_at older than 1 hour AND not referenced by any `bench_runs.status='running'` row) selects the orphaned gen.
- Refusal to drop a non-`bench:` generation: `DropGeneration` against a regular fingerprint returns an error and leaves rows intact.
- `Progress` callback fires with non-zero `ClaimElapsed`/`UpsertElapsed`/`CompleteElapsed` when the underlying queue/backend take measurable time (use a slow fake to ensure non-zero).
- Concurrent workers on one queue: 4 workers share the throwaway generation, each batch is claimed by exactly one worker, no double-Complete (existing `claim_token` semantics).

### `store`

- Lazy DDL: against an empty `vectors.db`, the first write-verb invocation creates the bench tables; read-verb invocations against a `vectors.db` without bench tables return a "no bench data" error rather than creating an empty schema.
- `bench_runs.config_hash` is canonical: re-ordering TOML keys before serializing produces the same hash. `canonical.go` enforces sorted-key JSON.
- Cascade behavior: `DELETE FROM bench_sweeps WHERE id = ?` cascades to `bench_runs` cascades to `bench_batches`. Tested via direct SQL.
- Migration idempotence: running DDL twice is a no-op (uses `CREATE TABLE IF NOT EXISTS`).

### CLI smoke

A subprocess test builds `embedshootout` and exercises one round-trip: `sample-create` → `run` (against an in-test fake server bound to localhost) → `show` → `compare` → `delete`. Asserts exit codes and JSON shape.

### BenchBackend (CreateBenchGeneration + DropGeneration)

In `internal/vector/sqlitevec/`:

- `CreateBenchGeneration(model="nomic", dim=768, runScope="7:42")` produces a generation with fingerprint `bench:7:42:nomic:768`, state `building`, and `seeded_at='bench-skipped'`. EnsureSeeded against this generation is a no-op.
- `CreateBenchGeneration` with empty `runScope` errors.
- `CreateBenchGeneration` does not insert any rows into `pending_embeddings` for the new gen (production seed pass is skipped).
- DropGeneration on a `bench:` generation removes vector rows + pending rows + generation row in one transaction.
- DropGeneration on a non-`bench:` generation errors and leaves rows untouched.
- DropGeneration on a non-existent generation errors with a clear message.
- Concurrent drop while a worker is `Claim`ing the same generation: the drop's vacate of `pending_embeddings` and the worker's `Claim` are both transactional against the same DB; one wins, the other gets "no rows" or a fresh empty result. Either is acceptable for benchmark teardown — bench callers always finish all workers before calling Drop, so this is a defense-in-depth test rather than a real path.
- Type-assertion smoke: `var _ vector.BenchBackend = (*sqlitevec.Backend)(nil)` compiles.

## Documentation

A short developer-doc note in `docs/` (e.g. `docs/embedshootout.md`) covering: how to install (`make embedshootout`), how to create a sample, how to run a sweep, how to read the output. Approximately one page; not user-facing documentation. CLAUDE.md gets a single-line entry under "Quick Commands" pointing at the doc.

## Open questions (deferred, non-blocking)

1. Whether `embedshootout sweep` should support pause/resume across reboots. For now: no — abort on signal, partial cells visible in `bench_runs` with `status='aborted'`, operator re-invokes; samples are durable and `config_hash` lets you skip already-done cells if anyone wants to add a `-skip-completed` flag later.
2. Whether to surface bench results in the TUI. Out of scope; `--json` is the integration surface.
3. Whether to bake a default `make bench-embed` target that runs a small canonical sweep. Probably yes once the harness has been used a few times and a good default exists; unnecessary now.
4. Whether `embed.ProgressReport`'s new optional fields (`ClaimElapsed`, `UpsertElapsed`, `CompleteElapsed`) should also be exposed in the existing `build-embeddings` progress line. They could provide useful diagnostic detail when the queue or backend is the bottleneck. Out of scope here — the windowed-ETA spec or a follow-up can decide.
