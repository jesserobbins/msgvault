# `embedshootout` Harness Implementation Plan

**Goal:** Build a developer-side embedding-performance harness — `embedshootout` — that defines stratified frozen sample sets, runs sweeps over endpoint × model × batch_size × workers × preprocess settings, and persists per-cell metrics in `vectors.db` for cross-run comparison. Two modes: **endpoint** (default; just `Client.Embed`) and **pipeline** (full `embed.Worker.RunOnce` against a throwaway `bench:`-prefixed generation).

**Architecture:** Mirrors `mimeshootout` — standalone binary at `scripts/embedshootout/main.go` invoking a library at `internal/vector/embed/bench/`. New optional `vector.BenchBackend` capability (alongside existing `FusingBackend`) gives the bench harness `CreateBenchGeneration` (no production seed) and `DropGeneration` (refuses non-`bench:` fingerprints). Production `vector.Backend` is unchanged. New tables in `vectors.db` are created lazily on first write — fresh DBs stay clean. The user-facing `msgvault` binary is untouched.

**Tech Stack:** Go 1.22+, SQLite (vec extension via `sqlite_vec` build tag), `mattn/go-sqlite3`, `BurntSushi/toml` (already a dep), stdlib `flag` (no cobra), existing `embed.Client` / `embed.Worker` / `embed.Preprocess`, existing `sqlitevec.Backend` lifecycle.

**Spec:** [`docs/specs/2026-04-25-embedshootout-harness-design.md`](../specs/2026-04-25-embedshootout-harness-design.md)

---

## Spec deviation: `seeded_at` sentinel

Spec §"Pipeline-mode runner" recommends stamping `seeded_at='bench-skipped'` (a string). The actual schema (`internal/vector/sqlitevec/schema.sql`) declares `seeded_at INTEGER` and `isGenerationSeeded` scans it as `sql.NullInt64` — a string would fail to scan. **Use `time.Now().Unix()` instead.** The bench gen is still unambiguously distinguishable from production via the `bench:` fingerprint prefix; no string sentinel is needed. The semantic outcome (`EnsureSeeded` is a no-op against bench gens) is identical.

---

## File Map

### Created

| File | Responsibility |
|------|----------------|
| `scripts/embedshootout/main.go` | Stdlib `flag` verb dispatcher: `sample-create`, `sample-list`, `sample-show`, `sample-delete`, `run`, `sweep`, `list`, `show`, `compare`, `delete` |
| `internal/vector/embed/bench/store.go` | DDL (lazy) + CRUD for `bench_samples`, `bench_sample_messages`, `bench_sweeps`, `bench_runs`, `bench_batches` |
| `internal/vector/embed/bench/store_test.go` | Lazy-DDL behavior, cascade deletes, idempotent migration, schema-absent error path for read verbs |
| `internal/vector/embed/bench/canonical.go` | Canonical-JSON helpers (`MarshalCanonical`, `ConfigHash`) |
| `internal/vector/embed/bench/canonical_test.go` | Re-ordered keys produce identical hash |
| `internal/vector/embed/bench/sample.go` | Sample CRUD; stratified sampler; deterministic `(seed, dim → ids)` |
| `internal/vector/embed/bench/sample_test.go` | Stratification math, empty-stratum error, seed reproducibility, cascade delete |
| `internal/vector/embed/bench/plan.go` | TOML plan parsing; `[[matrix]]` cartesian/zip expansion |
| `internal/vector/embed/bench/plan_test.go` | Cartesian product, model/dimension zip, mismatched-zip error, empty-axis rejection, round-trip |
| `internal/vector/embed/bench/metrics.go` | Per-batch aggregation, percentile (p50/p95/p99/max) helper |
| `internal/vector/embed/bench/metrics_test.go` | Known-distribution percentiles, msg/sec/ms/per/msg/us/per/char math, NaN-safe zero-divides |
| `internal/vector/embed/bench/runner.go` | Shared types: `RunInputs`, `BatchEvent`, `Aggregator`; classifier helpers (`classifyEmbedErr`) |
| `internal/vector/embed/bench/runner_endpoint.go` | Endpoint-mode runner: worker pool, warmup, error classification |
| `internal/vector/embed/bench/runner_endpoint_test.go` | Fake `EmbeddingClient` driving warmup, concurrency, error mix, all-empty corpus |
| `internal/vector/embed/bench/runner_pipeline.go` | Pipeline-mode runner: type-asserts `BenchBackend`, allocates `bench:` gen, drives `embed.Worker`s, drops on completion |
| `internal/vector/embed/bench/runner_pipeline_test.go` | Type-assert failure, no-seed, generation_id population, mid-cancel orphan cleanup, refuse-to-drop non-bench |
| `internal/vector/embed/bench/cleanup.go` | Orphan-`bench:` cleanup query + drop loop |
| `internal/vector/embed/bench/cleanup_test.go` | Selects only orphans older than threshold; respects `bench_runs.status='running'` |
| `internal/vector/embed/bench/report.go` | `show`/`list`/`compare` rendering (terminal + JSON) |
| `internal/vector/embed/bench/report_test.go` | Snapshot tests for terminal output; JSON shape for scripting |
| `internal/vector/embed/bench/run.go` | Top-level orchestrators `RunCell` (single cell) and `RunSweep` (matrix) wired to the runners |
| `internal/vector/embed/bench/run_test.go` | End-to-end smoke: sample → run → show round-trip with fakes |
| `internal/vector/sqlitevec/bench.go` | `Backend.CreateBenchGeneration` + `Backend.DropGeneration` impl; build-tagged `sqlite_vec` |
| `internal/vector/sqlitevec/bench_test.go` | Compile-time `var _ vector.BenchBackend = (*Backend)(nil)`; lifecycle tests |

### Modified

| File | Change |
|------|--------|
| `internal/vector/backend.go` | Add `BenchBackend` capability interface (after `FusingBackend`) |
| `internal/vector/embed/worker.go:70-77` | Extend `ProgressReport` with `ClaimElapsed`, `UpsertElapsed`, `CompleteElapsed time.Duration`; populate them where measurable in `RunOnce` |
| `internal/vector/embed/worker_test.go` | Existing tests assert new fields are zero by default; one new test populates them via slow fakes |
| `Makefile` | Add `embedshootout` and `run-embedshootout` targets; extend `clean`; update `help` |

### Why the bench package is split this way

Each file has one purpose. `runner.go` holds the shared event types so the two runners can be tested with a common harness. `runner_endpoint.go` and `runner_pipeline.go` are independent — endpoint can ship before pipeline if needed (and is the default mode anyway). `store.go` is pure SQL CRUD; `sample.go`, `plan.go`, `metrics.go` and `canonical.go` are pure-data utilities testable without any DB; `cleanup.go` and `report.go` are thin glue. This separation keeps individual files small (~150–300 lines each) so they fit in working context.

---

## Task ordering rationale

Tasks are ordered so each builds on durable prior work and so test-driven development has something concrete to assert against. Schema → canonical helper → backend interface → backend impl → ProgressReport additions → sample basic → plan parsing → endpoint runner (single worker) → endpoint runner (workers/warmup/errors) → pipeline runner → orphan cleanup → stratified sampler → reports → CLI dispatcher → Makefile/smoke. Stratified sampling lands *after* basic endpoint runs work because the simpler (unstratified) sampler is enough for early end-to-end smoke.

---

## Task 1: Lazy schema migration + `store.go` skeleton

**Files:**
- Create: `internal/vector/embed/bench/store.go`
- Create: `internal/vector/embed/bench/store_test.go`

- [ ] **Step 1: Write failing test for lazy DDL**

```go
package bench

import (
    "context"
    "database/sql"
    "errors"
    "testing"

    _ "github.com/mattn/go-sqlite3"
)

func openMem(t *testing.T) *sql.DB {
    t.Helper()
    db, err := sql.Open("sqlite3", ":memory:")
    if err != nil {
        t.Fatal(err)
    }
    t.Cleanup(func() { _ = db.Close() })
    return db
}

func TestEnsureSchema_CreatesTables(t *testing.T) {
    db := openMem(t)
    if err := EnsureSchema(context.Background(), db); err != nil {
        t.Fatalf("EnsureSchema: %v", err)
    }
    for _, tbl := range []string{
        "bench_samples", "bench_sample_messages",
        "bench_sweeps", "bench_runs", "bench_batches",
    } {
        var name string
        err := db.QueryRow(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`, tbl).Scan(&name)
        if err != nil {
            t.Errorf("table %s not created: %v", tbl, err)
        }
    }
}

func TestEnsureSchema_Idempotent(t *testing.T) {
    db := openMem(t)
    ctx := context.Background()
    if err := EnsureSchema(ctx, db); err != nil {
        t.Fatal(err)
    }
    if err := EnsureSchema(ctx, db); err != nil {
        t.Fatalf("second EnsureSchema: %v", err)
    }
}

func TestRequireSchema_ErrWhenAbsent(t *testing.T) {
    db := openMem(t)
    err := RequireSchema(context.Background(), db)
    if !errors.Is(err, ErrNoBenchData) {
        t.Fatalf("RequireSchema: got %v, want ErrNoBenchData", err)
    }
}
```

- [ ] **Step 2: Run test to verify failure**

`go test ./internal/vector/embed/bench/...` → expected: build failure (`bench` package does not exist).

- [ ] **Step 3: Create `store.go` with the schema and helpers**

```go
// Package bench implements the embedshootout sweep harness library:
// stratified samples, plan/matrix expansion, endpoint and pipeline
// mode runners, results storage, and reports. All tables are created
// lazily on first write so a fresh vectors.db stays clean of bench
// state until somebody actually opts in.
package bench

import (
    "context"
    "database/sql"
    "errors"
    "fmt"
)

// ErrNoBenchData is returned by RequireSchema when the bench tables
// have not been created yet. Read verbs (sample-list, list, show,
// compare) surface this with a clear "no bench data yet" message
// rather than silently materializing the schema.
var ErrNoBenchData = errors.New("bench: no bench data in this vectors.db (run sample-create or run a benchmark first)")

const benchSchemaSQL = `
CREATE TABLE IF NOT EXISTS bench_samples (
    name           TEXT PRIMARY KEY,
    created_at     INTEGER NOT NULL,
    size           INTEGER NOT NULL,
    stratify_spec  TEXT,
    seed           INTEGER,
    notes          TEXT
);
CREATE TABLE IF NOT EXISTS bench_sample_messages (
    sample_name    TEXT NOT NULL REFERENCES bench_samples(name) ON DELETE CASCADE,
    message_id     INTEGER NOT NULL,
    stratum        TEXT,
    PRIMARY KEY (sample_name, message_id)
);
CREATE TABLE IF NOT EXISTS bench_sweeps (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at     INTEGER NOT NULL,
    finished_at    INTEGER,
    status         TEXT NOT NULL,
    plan_path      TEXT,
    plan_toml      TEXT NOT NULL,
    git_sha        TEXT,
    notes          TEXT
);
CREATE TABLE IF NOT EXISTS bench_runs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    sweep_id        INTEGER REFERENCES bench_sweeps(id) ON DELETE CASCADE,
    started_at      INTEGER NOT NULL,
    finished_at     INTEGER,
    status          TEXT NOT NULL,
    error_message   TEXT,
    mode            TEXT NOT NULL,
    sample_name     TEXT NOT NULL,
    config_json     TEXT NOT NULL,
    config_hash     TEXT NOT NULL,
    cell_json       TEXT NOT NULL,
    -- generation_id is a historical/audit reference, NOT a foreign
    -- key. RunPipeline drops the throwaway generation before
    -- returning, so by the time bench_runs is committed the
    -- referenced index_generations row has already been deleted.
    -- Stored solely so post-hoc orphan investigations can correlate
    -- "this run touched gen 12345" with logs.
    generation_id   INTEGER,
    msgs_total      INTEGER,
    msgs_succeeded  INTEGER,
    msgs_truncated  INTEGER,
    msgs_dropped    INTEGER,
    chars_total     INTEGER,
    elapsed_ms      INTEGER,
    embed_ms_sum    INTEGER,
    msg_per_sec     REAL,
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
    retries         INTEGER,
    claim_ms_sum    INTEGER,
    upsert_ms_sum   INTEGER,
    complete_ms_sum INTEGER
);
CREATE INDEX IF NOT EXISTS bench_runs_sweep_idx       ON bench_runs(sweep_id);
CREATE INDEX IF NOT EXISTS bench_runs_config_hash_idx ON bench_runs(config_hash);

CREATE TABLE IF NOT EXISTS bench_batches (
    run_id      INTEGER NOT NULL REFERENCES bench_runs(id) ON DELETE CASCADE,
    seq         INTEGER NOT NULL,
    worker_id   INTEGER NOT NULL,
    msgs        INTEGER NOT NULL,
    chars       INTEGER NOT NULL,
    elapsed_ms  INTEGER NOT NULL,
    embed_ms    INTEGER NOT NULL,
    truncated   INTEGER NOT NULL,
    error       TEXT,
    is_warmup   INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (run_id, seq, worker_id)
);
`

// EnsureSchema creates the bench tables if they don't exist. Safe to
// call repeatedly. Foreign-key cascades require the connection to have
// PRAGMA foreign_keys = ON; the standard vectors.db open path enables
// this via sqlitevec.RegisterExtension's ConnectHook.
func EnsureSchema(ctx context.Context, db *sql.DB) error {
    if _, err := db.ExecContext(ctx, benchSchemaSQL); err != nil {
        return fmt.Errorf("bench: ensure schema: %w", err)
    }
    return nil
}

// RequireSchema reports ErrNoBenchData if the bench tables don't
// exist. Call from read verbs (sample-list, list, show, compare) so
// they don't silently materialize an empty schema.
func RequireSchema(ctx context.Context, db *sql.DB) error {
    var name string
    err := db.QueryRowContext(ctx,
        `SELECT name FROM sqlite_master WHERE type='table' AND name='bench_samples'`).Scan(&name)
    if errors.Is(err, sql.ErrNoRows) {
        return ErrNoBenchData
    }
    return err
}
```

- [ ] **Step 4: Run tests**

`go test ./internal/vector/embed/bench/...` → expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add internal/vector/embed/bench/store.go internal/vector/embed/bench/store_test.go
git commit -m "bench: add lazy schema migration for embedshootout tables"
```

---

## Task 2: Canonical-JSON `ConfigHash`

**Files:**
- Create: `internal/vector/embed/bench/canonical.go`
- Create: `internal/vector/embed/bench/canonical_test.go`

- [ ] **Step 1: Failing test**

```go
package bench

import "testing"

func TestConfigHash_OrderIndependent(t *testing.T) {
    a := map[string]any{"endpoint": "http://x", "model": "nomic", "batch_size": 32}
    b := map[string]any{"model": "nomic", "batch_size": 32, "endpoint": "http://x"}
    ha, err := ConfigHash(a)
    if err != nil {
        t.Fatal(err)
    }
    hb, err := ConfigHash(b)
    if err != nil {
        t.Fatal(err)
    }
    if ha != hb {
        t.Fatalf("ConfigHash differs: %s vs %s", ha, hb)
    }
}

func TestConfigHash_Deterministic(t *testing.T) {
    m := map[string]any{"endpoint": "http://x", "n": 1, "nested": map[string]any{"b": 2, "a": 1}}
    h1, _ := ConfigHash(m)
    h2, _ := ConfigHash(m)
    if h1 != h2 {
        t.Fatalf("not deterministic: %s vs %s", h1, h2)
    }
}

func TestConfigHash_NestedOrderIndependent(t *testing.T) {
    a := map[string]any{"x": map[string]any{"a": 1, "b": 2}}
    b := map[string]any{"x": map[string]any{"b": 2, "a": 1}}
    ha, _ := ConfigHash(a)
    hb, _ := ConfigHash(b)
    if ha != hb {
        t.Fatalf("nested order not normalized: %s vs %s", ha, hb)
    }
}
```

- [ ] **Step 2: Run, see failure** (`undefined: ConfigHash`).

- [ ] **Step 3: Implement**

```go
package bench

import (
    "crypto/sha256"
    "encoding/hex"
    "encoding/json"
    "fmt"
    "sort"
)

// MarshalCanonical produces a JSON encoding with object keys
// recursively sorted. This is the input to ConfigHash and is also what
// gets stored in bench_runs.config_json.
func MarshalCanonical(v any) ([]byte, error) {
    norm := canonicalize(v)
    return json.Marshal(norm)
}

// ConfigHash returns the hex-encoded sha256 of the canonical encoding.
func ConfigHash(v any) (string, error) {
    b, err := MarshalCanonical(v)
    if err != nil {
        return "", fmt.Errorf("bench: canonical marshal: %w", err)
    }
    sum := sha256.Sum256(b)
    return hex.EncodeToString(sum[:]), nil
}

// canonicalize walks v and replaces every map[string]any with a
// json.RawMessage whose key order is deterministic. Slices are
// preserved in input order (slice order is significant).
func canonicalize(v any) any {
    switch t := v.(type) {
    case map[string]any:
        keys := make([]string, 0, len(t))
        for k := range t {
            keys = append(keys, k)
        }
        sort.Strings(keys)
        out := make([]any, 0, len(keys)*2)
        // Build an ordered alternating key/value sequence and emit as
        // a JSON object via a small inline encoder so RawMessage round-
        // trips preserve order.
        // For simplicity here we use a sorted-key map encoding via an
        // intermediate slice of pairs serialized as a JSON object.
        type kv struct {
            K string
            V any
        }
        pairs := make([]kv, len(keys))
        for i, k := range keys {
            pairs[i] = kv{K: k, V: canonicalize(t[k])}
        }
        // json.Marshal a struct slice would serialize as an array; we
        // need an object. Build the map and rely on Go's stable
        // encoding for sorted-key maps in encoding/json (since Go 1.12,
        // json.Marshal of a map[string]X sorts keys alphabetically).
        m := make(map[string]any, len(pairs))
        for _, p := range pairs {
            m[p.K] = p.V
        }
        _ = out
        return m
    case []any:
        out := make([]any, len(t))
        for i, x := range t {
            out[i] = canonicalize(x)
        }
        return out
    default:
        return t
    }
}
```

Note: `encoding/json` since Go 1.12 sorts `map[string]X` keys alphabetically when marshaling, so the inner conversion to `map[string]any` after recursion is sufficient — the alphabetical-order behavior is guaranteed. The `keys`/`pairs` scaffolding is retained for clarity but the actual ordering is enforced by `json.Marshal` itself.

- [ ] **Step 4: Run tests** — PASS expected.

- [ ] **Step 5: Commit**

```bash
git add internal/vector/embed/bench/canonical.go internal/vector/embed/bench/canonical_test.go
git commit -m "bench: add canonical JSON + ConfigHash for cross-run dedup"
```

---

## Task 3: `vector.BenchBackend` interface

**Files:**
- Modify: `internal/vector/backend.go` (after `FusingBackend`)

- [ ] **Step 1: Add interface**

Append to `internal/vector/backend.go`:

```go
// BenchBackend is an optional capability for benchmark generation
// lifecycle: creating throwaway generations isolated from the
// production activate/retire flow, and dropping them safely. The
// embedshootout harness type-asserts a Backend to BenchBackend at
// run time; production code (sync, build-embeddings, hybrid search)
// does not see these methods and cannot accidentally call them. Same
// optional-capability pattern as FusingBackend.
type BenchBackend interface {
    Backend

    // CreateBenchGeneration creates a generation in `building` state
    // whose fingerprint is forced to begin with "bench:" — concretely
    // "bench:<runScope>:<model>:<dimension>" — so it cannot be
    // confused with a production generation. Implementations MUST
    // skip the production seed pass that CreateGeneration performs
    // (the bench caller seeds pending_embeddings explicitly with a
    // sample-bounded set), and MUST stamp seeded_at to a non-NULL
    // value so EnsureSeeded resume-paths treat the gen as already
    // seeded. The runScope must be non-empty.
    CreateBenchGeneration(ctx context.Context, model string, dimension int, runScope string) (GenerationID, error)

    // DropGeneration removes a generation and its associated rows
    // (vectors, pending_embeddings, index_generations row).
    // Implementations MUST verify the fingerprint begins with
    // "bench:" and return an error otherwise — this is a hard
    // contractual safety on top of any caller-side check.
    DropGeneration(ctx context.Context, gen GenerationID) error
}
```

- [ ] **Step 2: Build to confirm interface compiles**

`go build -tags "fts5 sqlite_vec" ./...` → expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add internal/vector/backend.go
git commit -m "vector: add BenchBackend optional capability interface"
```

---

## Task 4: `sqlitevec.Backend.CreateBenchGeneration` + `DropGeneration`

**Files:**
- Create: `internal/vector/sqlitevec/bench.go`
- Create: `internal/vector/sqlitevec/bench_test.go`

- [ ] **Step 1: Compile-time interface assertion + failing tests**

```go
//go:build sqlite_vec

package sqlitevec

import (
    "context"
    "database/sql"
    "errors"
    "fmt"
    "testing"

    "github.com/wesm/msgvault/internal/vector"
)

var _ vector.BenchBackend = (*Backend)(nil)

func TestCreateBenchGeneration_ForcesBenchPrefix(t *testing.T) {
    b, _ := openTestBackend(t)
    gen, err := b.CreateBenchGeneration(context.Background(), "nomic-embed-text", 768, "7:42")
    if err != nil {
        t.Fatalf("CreateBenchGeneration: %v", err)
    }
    var fp string
    if err := b.db.QueryRow(`SELECT fingerprint FROM index_generations WHERE id = ?`, int64(gen)).Scan(&fp); err != nil {
        t.Fatal(err)
    }
    if fp != "bench:7:42:nomic-embed-text:768" {
        t.Errorf("fingerprint: got %q, want %q", fp, "bench:7:42:nomic-embed-text:768")
    }
}

func TestCreateBenchGeneration_NoSeedPass(t *testing.T) {
    b, _ := openTestBackend(t)
    // Insert a row in main.messages so seedPending would have something
    // to copy over. (openTestBackend wires a main DB and inserts at
    // least one message in your existing test helpers.)
    gen, err := b.CreateBenchGeneration(context.Background(), "x", 8, "scope")
    if err != nil {
        t.Fatal(err)
    }
    var n int
    if err := b.db.QueryRow(`SELECT COUNT(*) FROM pending_embeddings WHERE generation_id = ?`, int64(gen)).Scan(&n); err != nil {
        t.Fatal(err)
    }
    if n != 0 {
        t.Errorf("bench gen seeded pending: got %d rows, want 0", n)
    }
}

func TestCreateBenchGeneration_EnsureSeededIsNoOp(t *testing.T) {
    b, _ := openTestBackend(t)
    gen, err := b.CreateBenchGeneration(context.Background(), "x", 8, "scope")
    if err != nil {
        t.Fatal(err)
    }
    // EnsureSeeded against a bench gen must NOT seedPending — verify
    // by checking pending_embeddings remains empty.
    if err := b.EnsureSeeded(context.Background(), gen); err != nil {
        t.Fatalf("EnsureSeeded against bench gen: %v", err)
    }
    var n int
    _ = b.db.QueryRow(`SELECT COUNT(*) FROM pending_embeddings WHERE generation_id = ?`, int64(gen)).Scan(&n)
    if n != 0 {
        t.Errorf("EnsureSeeded leaked pending: got %d rows, want 0", n)
    }
}

func TestCreateBenchGeneration_RejectsEmptyScope(t *testing.T) {
    b, _ := openTestBackend(t)
    _, err := b.CreateBenchGeneration(context.Background(), "x", 8, "")
    if err == nil {
        t.Fatal("expected error for empty runScope")
    }
}

func TestDropGeneration_BenchOnly(t *testing.T) {
    b, _ := openTestBackend(t)
    // bench gen — should drop cleanly.
    gen, _ := b.CreateBenchGeneration(context.Background(), "x", 8, "scope")
    if err := b.DropGeneration(context.Background(), gen); err != nil {
        t.Fatalf("DropGeneration bench: %v", err)
    }
    var exists int
    _ = b.db.QueryRow(`SELECT COUNT(*) FROM index_generations WHERE id = ?`, int64(gen)).Scan(&exists)
    if exists != 0 {
        t.Errorf("bench gen not deleted: %d rows", exists)
    }
}

func TestDropGeneration_RefusesNonBench(t *testing.T) {
    b, mainDB := openTestBackend(t)
    _ = mainDB
    // Create a regular production gen.
    gen, err := b.CreateGeneration(context.Background(), "real-model", 8)
    if err != nil {
        t.Fatal(err)
    }
    err = b.DropGeneration(context.Background(), gen)
    if err == nil {
        t.Fatal("DropGeneration accepted non-bench fingerprint")
    }
    // Production gen still exists.
    var exists int
    _ = b.db.QueryRow(`SELECT COUNT(*) FROM index_generations WHERE id = ?`, int64(gen)).Scan(&exists)
    if exists != 1 {
        t.Errorf("production gen wrongly deleted")
    }
}

func TestDropGeneration_UnknownGen(t *testing.T) {
    b, _ := openTestBackend(t)
    err := b.DropGeneration(context.Background(), 99999)
    if err == nil {
        t.Fatal("expected error for unknown gen")
    }
    if !errors.Is(err, sql.ErrNoRows) && err.Error() == "" {
        t.Errorf("error message should mention gen id; got %v", err)
    }
}
```

(`openTestBackend(t)` already exists in `backend_testhelpers_test.go` per the existing test infrastructure — reuse it. If its current signature doesn't return both backend and main DB, adjust the test to match what's there.)

- [ ] **Step 2: Run, observe failure**

`go test -tags "fts5 sqlite_vec" ./internal/vector/sqlitevec/...` → expected: undefined methods.

- [ ] **Step 3: Implement**

```go
//go:build sqlite_vec

package sqlitevec

import (
    "context"
    "database/sql"
    "errors"
    "fmt"
    "strings"
    "time"

    "github.com/wesm/msgvault/internal/vector"
)

const benchFingerprintPrefix = "bench:"

// CreateBenchGeneration implements vector.BenchBackend.
func (b *Backend) CreateBenchGeneration(ctx context.Context, model string, dim int, runScope string) (vector.GenerationID, error) {
    if strings.TrimSpace(runScope) == "" {
        return 0, fmt.Errorf("CreateBenchGeneration: runScope must be non-empty")
    }
    if err := EnsureVectorTable(ctx, b.db, dim); err != nil {
        return 0, err
    }
    fp := fmt.Sprintf("%s%s:%s:%d", benchFingerprintPrefix, runScope, model, dim)
    now := time.Now().Unix()

    // Insert directly: bench generations skip claimOrInsertBuilding's
    // resume / dual-enqueue dance. The unique partial index on
    // state='building' would normally prevent multiple concurrent
    // building gens; bench gens MUST share the building slot with the
    // production gen if one exists. To avoid that, bench gens are
    // inserted with state='building' BUT we accept that running a
    // pipeline-mode bench while a production rebuild is in progress
    // will race on the partial index. Document and reject that case.
    if existingFP, ok, err := b.lookupBuildingFingerprint(ctx); err != nil {
        return 0, err
    } else if ok {
        return 0, fmt.Errorf("CreateBenchGeneration: a building generation (%q) already exists; pipeline-mode bench cannot run during a production rebuild", existingFP)
    }

    res, err := b.db.ExecContext(ctx,
        `INSERT INTO index_generations
         (model, dimension, fingerprint, started_at, seeded_at, state)
         VALUES (?, ?, ?, ?, ?, 'building')`,
        model, dim, fp, now, now)
    if err != nil {
        return 0, fmt.Errorf("CreateBenchGeneration: insert: %w", err)
    }
    id, err := res.LastInsertId()
    if err != nil {
        return 0, fmt.Errorf("CreateBenchGeneration: last insert id: %w", err)
    }
    return vector.GenerationID(id), nil
}

// lookupBuildingFingerprint returns the fingerprint of any current
// building gen, or ok=false if none exists.
func (b *Backend) lookupBuildingFingerprint(ctx context.Context) (string, bool, error) {
    var fp string
    err := b.db.QueryRowContext(ctx,
        `SELECT fingerprint FROM index_generations WHERE state = 'building'`).Scan(&fp)
    if errors.Is(err, sql.ErrNoRows) {
        return "", false, nil
    }
    if err != nil {
        return "", false, fmt.Errorf("lookup building: %w", err)
    }
    return fp, true, nil
}

// DropGeneration implements vector.BenchBackend.
func (b *Backend) DropGeneration(ctx context.Context, gen vector.GenerationID) error {
    var fp string
    var dim int
    err := b.db.QueryRowContext(ctx,
        `SELECT fingerprint, dimension FROM index_generations WHERE id = ?`, int64(gen)).Scan(&fp, &dim)
    if errors.Is(err, sql.ErrNoRows) {
        return fmt.Errorf("DropGeneration: unknown generation %d: %w", gen, err)
    }
    if err != nil {
        return fmt.Errorf("DropGeneration: lookup: %w", err)
    }
    if !strings.HasPrefix(fp, benchFingerprintPrefix) {
        return fmt.Errorf("DropGeneration: refusing to drop non-bench generation %d (fingerprint=%q)", gen, fp)
    }

    tx, err := b.db.BeginTx(ctx, nil)
    if err != nil {
        return fmt.Errorf("DropGeneration: begin: %w", err)
    }
    defer func() { _ = tx.Rollback() }()

    // Vector data lives in two tables: the metadata `embeddings` table
    // and the dimension-specific vec0 virtual table.
    vecTable := VectorTableName(dim)
    if _, err := tx.ExecContext(ctx, `DELETE FROM embeddings WHERE generation_id = ?`, int64(gen)); err != nil {
        return fmt.Errorf("DropGeneration: delete embeddings: %w", err)
    }
    if _, err := tx.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s WHERE generation_id = ?`, vecTable), int64(gen)); err != nil {
        return fmt.Errorf("DropGeneration: delete %s: %w", vecTable, err)
    }
    if _, err := tx.ExecContext(ctx, `DELETE FROM pending_embeddings WHERE generation_id = ?`, int64(gen)); err != nil {
        return fmt.Errorf("DropGeneration: delete pending: %w", err)
    }
    if _, err := tx.ExecContext(ctx, `DELETE FROM index_generations WHERE id = ?`, int64(gen)); err != nil {
        return fmt.Errorf("DropGeneration: delete generation row: %w", err)
    }
    return tx.Commit()
}
```

- [ ] **Step 4: Run tests** — PASS expected.

- [ ] **Step 5: Commit**

```bash
git add internal/vector/sqlitevec/bench.go internal/vector/sqlitevec/bench_test.go
git commit -m "sqlitevec: implement CreateBenchGeneration + DropGeneration with bench: guard"
```

---

## Task 5: `ProgressReport` adds `ClaimElapsed` / `UpsertElapsed` / `CompleteElapsed`

**Files:**
- Modify: `internal/vector/embed/worker.go:70-77` (struct), and the Progress emission site in `RunOnce`
- Modify: `internal/vector/embed/worker_test.go`

- [ ] **Step 1: Failing test — fields populated**

Read `internal/vector/embed/testsupport_test.go` to find the existing fake `Backend` struct used by worker tests. Add a `UpsertSleep time.Duration` field to it (or wrap it with a slowing decorator if it's not directly extensible). The test must:

```go
func TestProgressReport_PipelineFieldsPopulated(t *testing.T) {
    // Wire a fake Backend whose Upsert sleeps 5ms; run a single batch.
    // Capture the ProgressReport via the Progress callback.
    // Assert: report.UpsertElapsed >= 4 * time.Millisecond
    //   (the strong assertion — driven by the injected sleep, robust
    //   to fast hardware).
    // Assert: report.ClaimElapsed >= 0 and report.CompleteElapsed >= 0
    //   (rules out the field being unwritten; SQLite queue ops can
    //   complete in sub-microsecond time on fast hardware so a
    //   strictly-positive threshold would be flaky).
}
```

The test must fail with the unmodified worker (zero-value fields) so Step 4 actually exercises the new measurement code.

- [ ] **Step 2: Extend the struct**

```go
type ProgressReport struct {
    Done            int
    TotalPending    int
    BatchMsgs       int
    BatchChars      int
    BatchElapsed    time.Duration
    RunElapsed      time.Duration
    // ClaimElapsed, UpsertElapsed, and CompleteElapsed are populated
    // when the worker measures these phases discretely. They default
    // to zero — printers that ignore them are unaffected. The bench
    // harness consumes them in pipeline mode to attribute time among
    // queue/backend/embedder.
    ClaimElapsed    time.Duration
    UpsertElapsed   time.Duration
    CompleteElapsed time.Duration
}
```

- [ ] **Step 3: Populate in `RunOnce`**

In the existing batch loop, capture `time.Now()` immediately before each of: `q.Claim`, `Backend.Upsert`, `q.Complete` (the success-path one), and pass the `time.Since` deltas into the `ProgressReport`. Reuse the existing `batchStart`/`runStart` machinery.

- [ ] **Step 4: Run tests, including the existing suite to confirm no regression**

`go test -tags "fts5 sqlite_vec" ./internal/vector/embed/...`

- [ ] **Step 5: Commit**

```bash
git add internal/vector/embed/worker.go internal/vector/embed/worker_test.go
git commit -m "embed: surface claim/upsert/complete phase durations on ProgressReport"
```

---

## Task 6: Sample CRUD (unstratified)

**Files:**
- Create: `internal/vector/embed/bench/sample.go`
- Create: `internal/vector/embed/bench/sample_test.go`

- [ ] **Step 1: Failing test**

```go
package bench

import (
    "context"
    "testing"
)

func TestCreateSample_Basic(t *testing.T) {
    db := openMem(t)
    ctx := context.Background()
    if err := EnsureSchema(ctx, db); err != nil {
        t.Fatal(err)
    }
    ids := []int64{1, 2, 3, 4, 5}
    err := CreateSampleFromIDs(ctx, db, "demo", ids, SampleMeta{Seed: 42, Notes: "hand-picked"})
    if err != nil {
        t.Fatalf("CreateSampleFromIDs: %v", err)
    }
    got, err := SampleMessageIDs(ctx, db, "demo")
    if err != nil {
        t.Fatal(err)
    }
    if len(got) != len(ids) {
        t.Fatalf("ids: got %d, want %d", len(got), len(ids))
    }
}

func TestCreateSample_DuplicateName(t *testing.T) {
    db := openMem(t)
    ctx := context.Background()
    _ = EnsureSchema(ctx, db)
    _ = CreateSampleFromIDs(ctx, db, "x", []int64{1}, SampleMeta{})
    err := CreateSampleFromIDs(ctx, db, "x", []int64{2}, SampleMeta{})
    if err == nil {
        t.Fatal("duplicate name accepted")
    }
}

func TestDeleteSample_Cascade(t *testing.T) {
    db := openMem(t)
    // Need foreign_keys=ON for cascade. SQLite ":memory:" with
    // mattn/go-sqlite3 honors PRAGMA but only per-connection; set it
    // here.
    if _, err := db.Exec(`PRAGMA foreign_keys = ON`); err != nil {
        t.Fatal(err)
    }
    ctx := context.Background()
    _ = EnsureSchema(ctx, db)
    _ = CreateSampleFromIDs(ctx, db, "x", []int64{1, 2, 3}, SampleMeta{})
    if err := DeleteSample(ctx, db, "x"); err != nil {
        t.Fatal(err)
    }
    var n int
    _ = db.QueryRow(`SELECT COUNT(*) FROM bench_sample_messages WHERE sample_name='x'`).Scan(&n)
    if n != 0 {
        t.Errorf("cascade failed: %d rows remain", n)
    }
}
```

- [ ] **Step 2: Run, see failure.**

- [ ] **Step 3: Implement**

```go
package bench

import (
    "context"
    "database/sql"
    "errors"
    "fmt"
    "time"
)

// SampleMeta carries the metadata stored on bench_samples: seed,
// stratify spec (canonical-JSON-encoded; nil when unstratified), and
// freeform notes.
type SampleMeta struct {
    Seed         int64
    StratifySpec string
    Notes        string
}

// CreateSampleFromIDs persists a sample with the given message IDs and
// optional per-ID stratum tags. Errors if the sample name already
// exists. Stratum tags must be the same length as ids, or nil.
func CreateSampleFromIDs(ctx context.Context, db *sql.DB, name string, ids []int64, meta SampleMeta, strata ...[]string) error {
    if name == "" {
        return fmt.Errorf("CreateSampleFromIDs: name must be non-empty")
    }
    if err := EnsureSchema(ctx, db); err != nil {
        return err
    }
    var stratum []string
    if len(strata) == 1 {
        stratum = strata[0]
        if len(stratum) != len(ids) {
            return fmt.Errorf("CreateSampleFromIDs: stratum length %d != ids length %d", len(stratum), len(ids))
        }
    } else if len(strata) > 1 {
        return fmt.Errorf("CreateSampleFromIDs: at most one stratum slice")
    }
    tx, err := db.BeginTx(ctx, nil)
    if err != nil {
        return err
    }
    defer func() { _ = tx.Rollback() }()
    var stratifyJSON sql.NullString
    if meta.StratifySpec != "" {
        stratifyJSON.String = meta.StratifySpec
        stratifyJSON.Valid = true
    }
    var seedNull sql.NullInt64
    if meta.Seed != 0 {
        seedNull = sql.NullInt64{Int64: meta.Seed, Valid: true}
    }
    if _, err := tx.ExecContext(ctx, `
        INSERT INTO bench_samples (name, created_at, size, stratify_spec, seed, notes)
        VALUES (?, ?, ?, ?, ?, ?)`,
        name, time.Now().Unix(), len(ids), stratifyJSON, seedNull, meta.Notes); err != nil {
        return fmt.Errorf("insert bench_samples: %w", err)
    }
    stmt, err := tx.PrepareContext(ctx,
        `INSERT INTO bench_sample_messages (sample_name, message_id, stratum) VALUES (?, ?, ?)`)
    if err != nil {
        return fmt.Errorf("prepare bench_sample_messages: %w", err)
    }
    defer func() { _ = stmt.Close() }()
    for i, id := range ids {
        var s sql.NullString
        if stratum != nil && stratum[i] != "" {
            s = sql.NullString{String: stratum[i], Valid: true}
        }
        if _, err := stmt.ExecContext(ctx, name, id, s); err != nil {
            return fmt.Errorf("insert sample message %d: %w", id, err)
        }
    }
    return tx.Commit()
}

// SampleMessageIDs returns the message IDs in the sample, in
// insertion order (PK order: sample_name, message_id, ascending).
func SampleMessageIDs(ctx context.Context, db *sql.DB, name string) ([]int64, error) {
    rows, err := db.QueryContext(ctx,
        `SELECT message_id FROM bench_sample_messages WHERE sample_name = ? ORDER BY message_id`, name)
    if err != nil {
        return nil, err
    }
    defer func() { _ = rows.Close() }()
    var ids []int64
    for rows.Next() {
        var id int64
        if err := rows.Scan(&id); err != nil {
            return nil, err
        }
        ids = append(ids, id)
    }
    return ids, rows.Err()
}

// SampleExists reports whether a sample with this name has been
// recorded.
func SampleExists(ctx context.Context, db *sql.DB, name string) (bool, error) {
    var n int
    err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM bench_samples WHERE name = ?`, name).Scan(&n)
    if errors.Is(err, sql.ErrNoRows) {
        return false, nil
    }
    return n > 0, err
}

// DeleteSample removes a sample (and cascade-deletes its message
// rows). PRAGMA foreign_keys must be ON for the cascade to fire —
// vectors.db has it enabled via the sqlitevec ConnectHook.
func DeleteSample(ctx context.Context, db *sql.DB, name string) error {
    _, err := db.ExecContext(ctx, `DELETE FROM bench_samples WHERE name = ?`, name)
    return err
}

// ListSamples returns all samples ordered by created_at desc.
func ListSamples(ctx context.Context, db *sql.DB) ([]SampleSummary, error) {
    if err := RequireSchema(ctx, db); err != nil {
        return nil, err
    }
    rows, err := db.QueryContext(ctx,
        `SELECT name, created_at, size, COALESCE(stratify_spec,''), COALESCE(seed,0), COALESCE(notes,'')
           FROM bench_samples ORDER BY created_at DESC`)
    if err != nil {
        return nil, err
    }
    defer func() { _ = rows.Close() }()
    var out []SampleSummary
    for rows.Next() {
        var s SampleSummary
        if err := rows.Scan(&s.Name, &s.CreatedAt, &s.Size, &s.StratifySpec, &s.Seed, &s.Notes); err != nil {
            return nil, err
        }
        out = append(out, s)
    }
    return out, rows.Err()
}

// SampleSummary is the row shape returned by ListSamples and
// sample-show.
type SampleSummary struct {
    Name         string
    CreatedAt    int64
    Size         int
    StratifySpec string
    Seed         int64
    Notes        string
}
```

- [ ] **Step 4: Run tests** — PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/vector/embed/bench/sample.go internal/vector/embed/bench/sample_test.go
git commit -m "bench: add unstratified sample CRUD with cascade-delete"
```

---

## Task 7: Plan TOML parser + matrix expander

**Files:**
- Create: `internal/vector/embed/bench/plan.go`
- Create: `internal/vector/embed/bench/plan_test.go`

- [ ] **Step 1: Failing test**

```go
package bench

import (
    "reflect"
    "strings"
    "testing"
)

const planMinimal = `
sample = "demo"
mode = "endpoint"

[[matrix]]
name = "endpoint"
values = ["http://a", "http://b"]

[[matrix]]
name = "model"
values = ["x", "y"]

[[matrix]]
name = "dimension"
values = [768, 1536]

[[matrix]]
name = "batch_size"
values = [16, 32]

[[matrix]]
name = "workers"
values = [1, 2]
`

func TestParsePlan_MatrixCells(t *testing.T) {
    p, err := ParsePlan([]byte(planMinimal))
    if err != nil {
        t.Fatal(err)
    }
    cells, err := p.Cells()
    if err != nil {
        t.Fatal(err)
    }
    // 2 endpoints × 2 (model+dim zipped) × 2 batch × 2 workers = 16
    if len(cells) != 16 {
        t.Errorf("cells: got %d, want 16", len(cells))
    }
}

func TestParsePlan_ModelDimensionZip_MismatchedLen(t *testing.T) {
    bad := strings.ReplaceAll(planMinimal, `values = [768, 1536]`, `values = [768]`)
    p, err := ParsePlan([]byte(bad))
    if err != nil {
        t.Fatal(err)
    }
    if _, err := p.Cells(); err == nil {
        t.Fatal("expected mismatched-zip error")
    }
}

func TestParsePlan_ModelDimensionZip_PositionPaired(t *testing.T) {
    p, err := ParsePlan([]byte(planMinimal))
    if err != nil {
        t.Fatal(err)
    }
    cells, _ := p.Cells()
    sawXAt768 := false
    sawY1536 := false
    sawXAt1536 := false
    for _, c := range cells {
        if c["model"] == "x" && reflect.DeepEqual(c["dimension"], int64(768)) {
            sawXAt768 = true
        }
        if c["model"] == "y" && reflect.DeepEqual(c["dimension"], int64(1536)) {
            sawY1536 = true
        }
        if c["model"] == "x" && reflect.DeepEqual(c["dimension"], int64(1536)) {
            sawXAt1536 = true
        }
    }
    if !sawXAt768 || !sawY1536 {
        t.Errorf("expected (x,768) and (y,1536) cells")
    }
    if sawXAt1536 {
        t.Errorf("zip violated: saw (x,1536)")
    }
}

func TestParsePlan_EmptyAxisRejected(t *testing.T) {
    bad := `
[[matrix]]
name = "endpoint"
values = []
`
    p, _ := ParsePlan([]byte(bad))
    if _, err := p.Cells(); err == nil {
        t.Fatal("expected empty-axis error")
    }
}
```

- [ ] **Step 2: Implement**

```go
package bench

import (
    "bytes"
    "fmt"

    "github.com/BurntSushi/toml"
)

// Plan is a parsed sweep plan: fixed parameters + a matrix of axes.
type Plan struct {
    Notes         string         `toml:"notes"`
    Sample        string         `toml:"sample"`
    Mode          string         `toml:"mode"` // "endpoint" | "pipeline"
    WarmupBatches int            `toml:"warmup_batches"`
    Fixed         map[string]any `toml:"fixed"`
    Matrix        []MatrixAxis   `toml:"matrix"`
}

// MatrixAxis declares one matrix dimension. Values are []any so we
// preserve the TOML scalar type (string/int/float).
type MatrixAxis struct {
    Name   string `toml:"name"`
    Values []any  `toml:"values"`
}

// ParsePlan parses TOML bytes into a Plan.
func ParsePlan(data []byte) (*Plan, error) {
    var p Plan
    if _, err := toml.NewDecoder(bytes.NewReader(data)).Decode(&p); err != nil {
        return nil, fmt.Errorf("ParsePlan: %w", err)
    }
    if p.Mode == "" {
        p.Mode = "endpoint"
    }
    if p.WarmupBatches == 0 {
        p.WarmupBatches = 1
    }
    return &p, nil
}

// Cell is one matrix cell: a map from axis name to scalar value.
type Cell map[string]any

// Cells expands the matrix into the cartesian product of axes, with
// the special-case that "model" and "dimension" are zipped position-
// wise (must have equal length).
func (p *Plan) Cells() ([]Cell, error) {
    if len(p.Matrix) == 0 {
        return nil, fmt.Errorf("plan has no matrix axes")
    }
    for _, ax := range p.Matrix {
        if len(ax.Values) == 0 {
            return nil, fmt.Errorf("matrix axis %q has no values", ax.Name)
        }
    }
    // Detect a model+dimension zip group. If both are present, fuse
    // them into a synthetic axis whose values are pair-maps.
    type fusedPair struct {
        Model string
        Dim   int64
    }
    var fused []fusedPair
    var modelIdx, dimIdx int = -1, -1
    for i, ax := range p.Matrix {
        if ax.Name == "model" {
            modelIdx = i
        }
        if ax.Name == "dimension" {
            dimIdx = i
        }
    }
    if modelIdx >= 0 && dimIdx >= 0 {
        ms, ds := p.Matrix[modelIdx].Values, p.Matrix[dimIdx].Values
        if len(ms) != len(ds) {
            return nil, fmt.Errorf("matrix axes 'model' and 'dimension' must have equal length when both present (got %d and %d)", len(ms), len(ds))
        }
        for i := range ms {
            ms_str, ok := ms[i].(string)
            if !ok {
                return nil, fmt.Errorf("matrix axis 'model' value %d is not a string: %v", i, ms[i])
            }
            d, err := toInt64(ds[i])
            if err != nil {
                return nil, fmt.Errorf("matrix axis 'dimension' value %d: %w", i, err)
            }
            fused = append(fused, fusedPair{Model: ms_str, Dim: d})
        }
    }
    // Build the iteration plan: all axes except model/dim, plus the
    // synthetic fused axis when applicable.
    type axis struct {
        Name   string
        Values []any
    }
    var axes []axis
    for i, ax := range p.Matrix {
        if i == modelIdx || i == dimIdx {
            continue
        }
        axes = append(axes, axis{Name: ax.Name, Values: ax.Values})
    }
    if len(fused) > 0 {
        // Synthesize an axis whose values are []any of fused pairs,
        // expanded into the cell map by the recursive walker below.
        anyVals := make([]any, len(fused))
        for i, fp := range fused {
            anyVals[i] = fp
        }
        axes = append(axes, axis{Name: "__model_dim__", Values: anyVals})
    }
    // Recursive cartesian walk.
    var out []Cell
    var rec func(i int, cur Cell)
    rec = func(i int, cur Cell) {
        if i == len(axes) {
            // Materialize fused entry into the cell.
            cell := make(Cell, len(cur))
            for k, v := range cur {
                if k == "__model_dim__" {
                    fp := v.(fusedPair)
                    cell["model"] = fp.Model
                    cell["dimension"] = fp.Dim
                } else {
                    cell[k] = v
                }
            }
            out = append(out, cell)
            return
        }
        for _, v := range axes[i].Values {
            cur[axes[i].Name] = v
            rec(i+1, cur)
            delete(cur, axes[i].Name)
        }
    }
    rec(0, Cell{})
    return out, nil
}

// toInt64 normalizes TOML's int representations (which the BurntSushi
// decoder hands us as int64 already, but be defensive).
func toInt64(v any) (int64, error) {
    switch x := v.(type) {
    case int64:
        return x, nil
    case int:
        return int64(x), nil
    case float64:
        return int64(x), nil
    default:
        return 0, fmt.Errorf("not an int: %T %v", v, v)
    }
}
```

- [ ] **Step 3: Run tests** — PASS.

- [ ] **Step 4: Commit**

```bash
git add internal/vector/embed/bench/plan.go internal/vector/embed/bench/plan_test.go
git commit -m "bench: add plan TOML parser with model/dimension zip semantics"
```

---

## Task 8: Per-batch metrics aggregation + percentiles

**Files:**
- Create: `internal/vector/embed/bench/metrics.go`
- Create: `internal/vector/embed/bench/metrics_test.go`

- [ ] **Step 1: Failing test (percentile correctness, NaN safety)**

```go
package bench

import (
    "math"
    "testing"
    "time"
)

func TestPercentile_KnownDistribution(t *testing.T) {
    // 100 samples uniform in [50, 150) ms.
    var ds []time.Duration
    for i := 0; i < 100; i++ {
        ds = append(ds, time.Duration(50+i)*time.Millisecond)
    }
    p50 := PercentileMs(ds, 50)
    if math.Abs(p50-99.5) > 1.0 {
        t.Errorf("p50: got %.2f, want ~99.5", p50)
    }
    p95 := PercentileMs(ds, 95)
    if math.Abs(p95-144.5) > 1.0 {
        t.Errorf("p95: got %.2f, want ~144.5", p95)
    }
}

func TestPercentile_Empty(t *testing.T) {
    if !math.IsNaN(PercentileMs(nil, 50)) {
        t.Errorf("empty input should yield NaN, got %v", PercentileMs(nil, 50))
    }
}

func TestAggregate_NoNaNOnEmpty(t *testing.T) {
    a := NewAggregator()
    r := a.Result()
    if r.MsgPerSec != 0 || r.UsPerChar != 0 {
        t.Errorf("zero aggregator should yield zero rates, got %+v", r)
    }
}
```

- [ ] **Step 2: Implement**

```go
package bench

import (
    "math"
    "sort"
    "time"
)

// Aggregator accumulates per-batch metrics during a run and produces
// a final summary. Only non-warmup, non-error batches contribute to
// throughput aggregates; error counters are accumulated separately.
type Aggregator struct {
    // batches contributes to throughput / percentile aggregates.
    batchElapsed []time.Duration
    msgs         int
    chars        int
    embedTotal   time.Duration
    truncated    int
    // error counters
    errors4xx, errors5xx, errors429, errorsNet int
    retries                                    int
    // pipeline-only sums
    claimMs, upsertMs, completeMs time.Duration
    // overall wall time (warmup INCLUDED)
    runStart   time.Time
    runStarted bool
    runEnd     time.Time
}

func NewAggregator() *Aggregator { return &Aggregator{} }

// Start records the run start.
func (a *Aggregator) Start() {
    a.runStart = time.Now()
    a.runStarted = true
}

// Stop records the run end.
func (a *Aggregator) Stop() { a.runEnd = time.Now() }

// AddBatch records a successful, non-warmup batch.
func (a *Aggregator) AddBatch(msgs, chars int, elapsed, embed, claim, upsert, complete time.Duration, truncated int) {
    a.batchElapsed = append(a.batchElapsed, elapsed)
    a.msgs += msgs
    a.chars += chars
    a.embedTotal += embed
    a.truncated += truncated
    a.claimMs += claim
    a.upsertMs += upsert
    a.completeMs += complete
}

// AddError records a batch that errored (warmup or not).
func (a *Aggregator) AddError(class string) {
    switch class {
    case "4xx":
        a.errors4xx++
    case "5xx":
        a.errors5xx++
    case "429":
        a.errors429++
    case "network":
        a.errorsNet++
    }
}

// AddRetries adds N to the retry counter.
func (a *Aggregator) AddRetries(n int) { a.retries += n }

// Aggregate is the final summary the runner stores.
type Aggregate struct {
    Msgs                 int
    Chars                int
    Truncated            int
    ElapsedMs            int64 // wall time including warmup
    EmbedMsSum           int64 // sum of embed durations excluding warmup
    MsgPerSec            float64
    MsPerMsg             float64
    UsPerChar            float64
    BatchP50, BatchP95, BatchP99, BatchMax float64
    Errors4xx, Errors5xx, Errors429, ErrorsNet int
    Retries              int
    ClaimMs, UpsertMs, CompleteMs int64
}

func (a *Aggregator) Result() Aggregate {
    var r Aggregate
    if a.runStarted {
        r.ElapsedMs = a.runEnd.Sub(a.runStart).Milliseconds()
    }
    r.Msgs = a.msgs
    r.Chars = a.chars
    r.Truncated = a.truncated
    r.EmbedMsSum = a.embedTotal.Milliseconds()
    if a.embedTotal > 0 && a.msgs > 0 {
        r.MsgPerSec = float64(a.msgs) / a.embedTotal.Seconds()
        r.MsPerMsg = a.embedTotal.Seconds() * 1000.0 / float64(a.msgs)
    }
    if a.embedTotal > 0 && a.chars > 0 {
        r.UsPerChar = a.embedTotal.Seconds() * 1_000_000.0 / float64(a.chars)
    }
    r.BatchP50 = PercentileMs(a.batchElapsed, 50)
    r.BatchP95 = PercentileMs(a.batchElapsed, 95)
    r.BatchP99 = PercentileMs(a.batchElapsed, 99)
    r.BatchMax = MaxMs(a.batchElapsed)
    r.Errors4xx = a.errors4xx
    r.Errors5xx = a.errors5xx
    r.Errors429 = a.errors429
    r.ErrorsNet = a.errorsNet
    r.Retries = a.retries
    r.ClaimMs = a.claimMs.Milliseconds()
    r.UpsertMs = a.upsertMs.Milliseconds()
    r.CompleteMs = a.completeMs.Milliseconds()
    return r
}

// PercentileMs returns the requested percentile of ds (in ms).
// Linear interpolation between adjacent ranks. Returns NaN on empty.
func PercentileMs(ds []time.Duration, p float64) float64 {
    if len(ds) == 0 {
        return math.NaN()
    }
    sorted := append([]time.Duration(nil), ds...)
    sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
    if p <= 0 {
        return float64(sorted[0]) / float64(time.Millisecond)
    }
    if p >= 100 {
        return float64(sorted[len(sorted)-1]) / float64(time.Millisecond)
    }
    rank := (p / 100.0) * float64(len(sorted)-1)
    lo := int(math.Floor(rank))
    hi := int(math.Ceil(rank))
    if lo == hi {
        return float64(sorted[lo]) / float64(time.Millisecond)
    }
    frac := rank - float64(lo)
    a := float64(sorted[lo])
    b := float64(sorted[hi])
    return (a + (b-a)*frac) / float64(time.Millisecond)
}

// MaxMs returns the maximum (in ms), or NaN on empty.
func MaxMs(ds []time.Duration) float64 {
    if len(ds) == 0 {
        return math.NaN()
    }
    var m time.Duration
    for _, d := range ds {
        if d > m {
            m = d
        }
    }
    return float64(m) / float64(time.Millisecond)
}
```

- [ ] **Step 3: Run tests** — PASS.

- [ ] **Step 4: Commit**

```bash
git add internal/vector/embed/bench/metrics.go internal/vector/embed/bench/metrics_test.go
git commit -m "bench: add percentile + per-batch aggregator with NaN-safe rates"
```

---

## Task 9: Endpoint runner — single worker, no warmup

**Files:**
- Create: `internal/vector/embed/bench/runner.go` (shared types)
- Create: `internal/vector/embed/bench/runner_endpoint.go`
- Create: `internal/vector/embed/bench/runner_endpoint_test.go`

- [ ] **Step 1: Failing test — single worker fast happy path**

```go
package bench

import (
    "context"
    "errors"
    "testing"
    "time"
)

type fakeEmbedClient struct {
    embedDur  time.Duration
    err       error
    callCount int
    dim       int
}

func (f *fakeEmbedClient) Embed(ctx context.Context, inputs []string) ([][]float32, error) {
    f.callCount++
    if f.err != nil {
        return nil, f.err
    }
    time.Sleep(f.embedDur)
    out := make([][]float32, len(inputs))
    for i := range out {
        out[i] = make([]float32, f.dim)
    }
    return out, nil
}

func TestRunEndpoint_SingleWorker_HappyPath(t *testing.T) {
    msgs := []PreparedMessage{
        {ID: 1, Text: "alpha", Chars: 5},
        {ID: 2, Text: "bravo", Chars: 5},
        {ID: 3, Text: "charlie", Chars: 7},
        {ID: 4, Text: "delta", Chars: 5},
    }
    fake := &fakeEmbedClient{dim: 8, embedDur: 5 * time.Millisecond}
    res, err := RunEndpoint(context.Background(), EndpointInputs{
        Messages:      msgs,
        Client:        fake,
        BatchSize:     2,
        Workers:       1,
        WarmupBatches: 0,
    })
    if err != nil {
        t.Fatalf("RunEndpoint: %v", err)
    }
    agg := res.Result()
    if agg.Msgs != 4 {
        t.Errorf("Msgs: got %d, want 4", agg.Msgs)
    }
    if fake.callCount != 2 {
        t.Errorf("callCount: got %d, want 2 (4 msgs / batch 2)", fake.callCount)
    }
}

func TestRunEndpoint_PropagatesContextCancel(t *testing.T) {
    fake := &fakeEmbedClient{dim: 8, embedDur: 100 * time.Millisecond}
    ctx, cancel := context.WithCancel(context.Background())
    cancel()
    _, err := RunEndpoint(ctx, EndpointInputs{
        Messages:  []PreparedMessage{{ID: 1, Text: "x", Chars: 1}},
        Client:    fake,
        BatchSize: 1,
        Workers:   1,
    })
    if !errors.Is(err, context.Canceled) {
        t.Fatalf("err: got %v, want context.Canceled", err)
    }
}
```

- [ ] **Step 2: Implement shared types in `runner.go`**

```go
package bench

import (
    "context"
    "errors"
    "fmt"
    "net"
    "strings"

    "github.com/wesm/msgvault/internal/vector/embed"
)

// PreparedMessage is the input to a runner — already preprocessed,
// with truncation already applied. Empty Text means the message was
// dropped during preprocess and should not enter the queue.
type PreparedMessage struct {
    ID    int64
    Text  string
    Chars int
    Trunc bool
}

// EmbedClient is the subset of *embed.Client used by the endpoint
// runner; allowing tests to inject a fake. It deliberately matches
// embed.EmbeddingClient.
type EmbedClient = embed.EmbeddingClient

// classifyEmbedErr maps a non-nil error from EmbedClient.Embed to one
// of: "4xx", "5xx", "429", "network", "other".
func classifyEmbedErr(err error) string {
    if err == nil {
        return ""
    }
    if errors.Is(err, embed.ErrPermanent4xx) {
        return "4xx"
    }
    var netErr net.Error
    if errors.As(err, &netErr) {
        return "network"
    }
    msg := err.Error()
    if strings.Contains(msg, "429") {
        return "429"
    }
    if strings.Contains(msg, "5xx") || strings.Contains(msg, "500 ") || strings.Contains(msg, "502 ") || strings.Contains(msg, "503 ") || strings.Contains(msg, "504 ") {
        return "5xx"
    }
    return "other"
}

// rangedError is a tiny helper to format a multi-failure error.
func joinErrs(errs []error) error {
    if len(errs) == 0 {
        return nil
    }
    if len(errs) == 1 {
        return errs[0]
    }
    var msg strings.Builder
    msg.WriteString(fmt.Sprintf("%d errors: ", len(errs)))
    for i, e := range errs {
        if i > 0 {
            msg.WriteString("; ")
        }
        msg.WriteString(e.Error())
    }
    return errors.New(msg.String())
}
```

(`embed.ErrPermanent4xx` already exists in `internal/vector/embed/client.go` — the classifier can reference it directly.)

- [ ] **Step 3: Implement endpoint runner in `runner_endpoint.go`**

```go
package bench

import (
    "context"
    "fmt"
    "sync"
    "time"
)

// EndpointInputs is the parameter bundle for the endpoint runner.
type EndpointInputs struct {
    Messages      []PreparedMessage
    Client        EmbedClient
    BatchSize     int
    Workers       int
    WarmupBatches int
}

// RunEndpoint runs N workers consuming preprocessed messages and
// emitting batches against the EmbedClient. Returns when all messages
// are consumed (success or error) or ctx is cancelled.
func RunEndpoint(ctx context.Context, in EndpointInputs) (*Aggregator, error) {
    if in.BatchSize <= 0 {
        return nil, fmt.Errorf("RunEndpoint: BatchSize must be > 0")
    }
    if in.Workers <= 0 {
        return nil, fmt.Errorf("RunEndpoint: Workers must be > 0")
    }
    agg := NewAggregator()
    agg.Start()
    defer agg.Stop()

    ch := make(chan PreparedMessage, in.BatchSize*in.Workers*2)
    feedDone := make(chan struct{})
    go func() {
        defer close(ch)
        defer close(feedDone)
        for _, m := range in.Messages {
            select {
            case <-ctx.Done():
                return
            case ch <- m:
            }
        }
    }()

    var (
        wg      sync.WaitGroup
        mu      sync.Mutex
        runErr  error
    )
    for w := 0; w < in.Workers; w++ {
        wg.Add(1)
        go func(wid int) {
            defer wg.Done()
            warmupLeft := in.WarmupBatches
            var batch []PreparedMessage
            for {
                select {
                case <-ctx.Done():
                    mu.Lock()
                    runErr = ctx.Err()
                    mu.Unlock()
                    return
                case m, ok := <-ch:
                    if !ok {
                        if len(batch) > 0 {
                            sendBatch(ctx, batch, in.Client, agg, &warmupLeft)
                            batch = batch[:0]
                        }
                        return
                    }
                    batch = append(batch, m)
                    if len(batch) >= in.BatchSize {
                        sendBatch(ctx, batch, in.Client, agg, &warmupLeft)
                        batch = batch[:0]
                    }
                }
            }
        }(w)
    }
    wg.Wait()
    if runErr != nil {
        return agg, runErr
    }
    return agg, nil
}

// sendBatch is a shared helper used by RunEndpoint workers; it embeds
// the batch (timing it), updates aggregator counters, and decrements
// warmupLeft. Errors are recorded into the aggregator and the worker
// continues — the run does not abort on individual batch failures.
func sendBatch(ctx context.Context, batch []PreparedMessage, c EmbedClient, agg *Aggregator, warmupLeft *int) {
    inputs := make([]string, len(batch))
    chars := 0
    truncated := 0
    for i, m := range batch {
        inputs[i] = m.Text
        chars += m.Chars
        if m.Trunc {
            truncated++
        }
    }
    start := time.Now()
    _, err := c.Embed(ctx, inputs)
    elapsed := time.Since(start)
    isWarmup := *warmupLeft > 0
    if isWarmup {
        *warmupLeft--
    }
    if err != nil {
        cls := classifyEmbedErr(err)
        if cls != "" {
            agg.AddError(cls)
        }
        return
    }
    if isWarmup {
        return
    }
    agg.AddBatch(len(batch), chars, elapsed, elapsed, 0, 0, 0, truncated)
}
```

- [ ] **Step 4: Run tests** — PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/vector/embed/bench/runner.go internal/vector/embed/bench/runner_endpoint.go internal/vector/embed/bench/runner_endpoint_test.go
git commit -m "bench: add endpoint-mode runner with single-worker happy path"
```

---

## Task 10: Endpoint runner — warmup, multi-worker, error classification

**Files:**
- Modify: `internal/vector/embed/bench/runner_endpoint_test.go`

- [ ] **Step 1: Add warmup test** — first `WarmupBatches` per worker excluded from aggregates but still embedded.
- [ ] **Step 2: Add multi-worker test** — 4 workers + slow fake → wall time < sequential baseline.
- [ ] **Step 3: Add error-class test** — fake returns a mix of `embed.ErrPermanent4xx`, a 500 string, a 429 string, and a `*net.OpError` (use `&net.OpError{Op: "dial", Err: errors.New("connection refused")}`); verify counters match.
- [ ] **Step 4: Add all-empty test** — all messages have `Text == ""` (the *runner's* responsibility is to receive only non-empty messages; verify the runner skips them gracefully — the actual filtering happens in Task 11's runner driver).
- [ ] **Step 5: All tests PASS without changing the runner code (the implementation in Task 9 already handles these).**
- [ ] **Step 6: Commit**

```bash
git add internal/vector/embed/bench/runner_endpoint_test.go
git commit -m "bench: cover endpoint runner warmup, concurrency, error classification"
```

---

## Task 11: Pipeline-mode runner

**Files:**
- Create: `internal/vector/embed/bench/runner_pipeline.go`
- Create: `internal/vector/embed/bench/runner_pipeline_test.go`

- [ ] **Step 1: Failing tests covering**
  - Backend that doesn't implement `BenchBackend` → clear error.
  - Throwaway gen created with `bench:` prefix and dropped on success.
  - `pending_embeddings` populated with sample IDs only (not the corpus).
  - `bench_runs.generation_id` is populated.
  - Mid-run `ctx.Cancel()` leaves the gen behind (its `started_at` is in the past, so a subsequent cleanup pass would drop it).

- [ ] **Step 2: Implement runner**

```go
package bench

import (
    "context"
    "database/sql"
    "fmt"
    "time"

    "github.com/wesm/msgvault/internal/vector"
    "github.com/wesm/msgvault/internal/vector/embed"
)

// PipelineInputs is the parameter bundle for the pipeline runner.
type PipelineInputs struct {
    SampleIDs    []int64
    BackendBase  vector.Backend // type-asserted to BenchBackend
    MainDB       *sql.DB
    VectorsDB    *sql.DB
    Client       embed.EmbeddingClient
    Preprocess   embed.PreprocessConfig
    MaxInputChars int
    BatchSize     int
    Workers       int
    Model         string
    Dimension     int
    RunScope      string // "<sweep_id>:<run_id>"
}

// PipelineResult carries the aggregator and the throwaway gen ID so
// the caller can record bench_runs.generation_id. RunPipeline ALWAYS
// drops the gen before returning (success, error, or cancel), so the
// caller never owns drop responsibility — but the gen ID is exposed
// so callers can persist it on bench_runs for after-the-fact orphan
// auditability.
type PipelineResult struct {
    Aggregator   *Aggregator
    GenerationID vector.GenerationID
}

// RunPipeline drives N embed.Worker instances against a throwaway
// bench: generation and drops the gen on every return path.
func RunPipeline(ctx context.Context, in PipelineInputs) (result *PipelineResult, err error) {
    bb, ok := in.BackendBase.(vector.BenchBackend)
    if !ok {
        return nil, fmt.Errorf("RunPipeline: backend does not support BenchBackend (pipeline-mode benchmarking unavailable)")
    }
    gen, err := bb.CreateBenchGeneration(ctx, in.Model, in.Dimension, in.RunScope)
    if err != nil {
        return nil, fmt.Errorf("RunPipeline: create bench gen: %w", err)
    }
    // Always drop the throwaway gen before returning, even on cancel.
    // Use a fresh context for the drop so an already-cancelled run
    // still cleans up; bound it to a short deadline so a stuck
    // backend can't hang the harness indefinitely. The `err == nil`
    // gate exists because Go's named-return semantics let the defer
    // overwrite `err` — we only want the drop error to surface when
    // the run itself succeeded, otherwise the run's error is more
    // informative.
    defer func() {
        dropCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
        defer cancel()
        if dErr := bb.DropGeneration(dropCtx, gen); dErr != nil && err == nil {
            err = fmt.Errorf("RunPipeline: drop bench gen: %w", dErr)
        }
    }()

    if err := seedSamplePending(ctx, in.VectorsDB, gen, in.SampleIDs); err != nil {
        return &PipelineResult{GenerationID: gen}, fmt.Errorf("RunPipeline: seed pending: %w", err)
    }

    agg := NewAggregator()
    agg.Start()
    defer agg.Stop()

    progress := make(chan embed.ProgressReport, in.Workers*4)
    done := make(chan error, in.Workers)
    for w := 0; w < in.Workers; w++ {
        go func() {
            worker := embed.NewWorker(embed.WorkerDeps{
                Backend:       in.BackendBase,
                VectorsDB:     in.VectorsDB,
                MainDB:        in.MainDB,
                Client:        in.Client,
                Preprocess:    in.Preprocess,
                MaxInputChars: in.MaxInputChars,
                BatchSize:     in.BatchSize,
                Progress:      func(r embed.ProgressReport) { progress <- r },
            })
            _, err := worker.RunOnce(ctx, gen)
            done <- err
        }()
    }

    // Drain progress until all workers signal done. After the last
    // 'done' arrives we drain any remaining buffered progress events
    // so we don't lose tail telemetry.
    closed := 0
    var firstWorkerErr error
    for closed < in.Workers {
        select {
        case r := <-progress:
            recordPipelineBatch(agg, r)
        case e := <-done:
            closed++
            if e != nil && ctx.Err() == nil && firstWorkerErr == nil {
                firstWorkerErr = e
            }
        }
    }
    // Drain any leftover progress events that landed after the last 'done'.
drain:
    for {
        select {
        case r := <-progress:
            recordPipelineBatch(agg, r)
        default:
            break drain
        }
    }
    if firstWorkerErr != nil {
        return &PipelineResult{Aggregator: agg, GenerationID: gen}, firstWorkerErr
    }
    return &PipelineResult{Aggregator: agg, GenerationID: gen}, ctx.Err()
}

// recordPipelineBatch attributes a ProgressReport's elapsed time
// across embed/claim/upsert/complete buckets. The "embed" bucket is
// BatchElapsed minus the discrete phase durations, clamped at zero —
// negatives can occur when phase fields are unmeasured (zero-valued)
// in a worker that hasn't been updated, or from measurement skew.
func recordPipelineBatch(agg *Aggregator, r embed.ProgressReport) {
    embedDur := r.BatchElapsed - r.ClaimElapsed - r.UpsertElapsed - r.CompleteElapsed
    if embedDur < 0 {
        embedDur = 0
    }
    agg.AddBatch(r.BatchMsgs, r.BatchChars, r.BatchElapsed, embedDur,
        r.ClaimElapsed, r.UpsertElapsed, r.CompleteElapsed, 0)
}

func seedSamplePending(ctx context.Context, db *sql.DB, gen vector.GenerationID, ids []int64) error {
    tx, err := db.BeginTx(ctx, nil)
    if err != nil {
        return err
    }
    defer func() { _ = tx.Rollback() }()
    stmt, err := tx.PrepareContext(ctx,
        `INSERT OR IGNORE INTO pending_embeddings (generation_id, message_id, enqueued_at) VALUES (?, ?, ?)`)
    if err != nil {
        return err
    }
    defer func() { _ = stmt.Close() }()
    now := time.Now().Unix()
    for _, id := range ids {
        if _, err := stmt.ExecContext(ctx, int64(gen), id, now); err != nil {
            return err
        }
    }
    return tx.Commit()
}
```

- [ ] **Step 3: Tests pass** — verify against an in-memory `vectors.db` plus a stub `BenchBackend` (implement a minimal in-test stub that satisfies the interface and tracks calls).
- [ ] **Step 4: Commit**

```bash
git add internal/vector/embed/bench/runner_pipeline.go internal/vector/embed/bench/runner_pipeline_test.go
git commit -m "bench: add pipeline-mode runner driving N workers against throwaway bench: gen"
```

---

## Task 12: Orphan-`bench:` cleanup

**Files:**
- Create: `internal/vector/embed/bench/cleanup.go`
- Create: `internal/vector/embed/bench/cleanup_test.go`

- [ ] **Step 1: Failing test**

Cover: a `bench:` gen older than threshold with no running run is selected; a `bench:` gen referenced by a `running` `bench_runs` row is NOT selected; a `bench:` gen newer than threshold is NOT selected; a non-`bench:` gen is never selected.

- [ ] **Step 2: Implement**

```go
package bench

import (
    "context"
    "database/sql"
    "fmt"
    "time"

    "github.com/wesm/msgvault/internal/vector"
)

// CleanupOrphanGenerations finds bench: generations older than
// olderThan and not referenced by any running bench_runs row, and
// drops each via BenchBackend.DropGeneration. Returns the count of
// dropped generations.
func CleanupOrphanGenerations(ctx context.Context, db *sql.DB, bb vector.BenchBackend, olderThan time.Duration) (int, error) {
    cutoff := time.Now().Add(-olderThan).Unix()
    rows, err := db.QueryContext(ctx, `
        SELECT g.id
          FROM index_generations g
         WHERE g.fingerprint LIKE 'bench:%'
           AND g.started_at < ?
           AND NOT EXISTS (
               SELECT 1 FROM bench_runs r
                WHERE r.generation_id = g.id
                  AND r.status = 'running')`, cutoff)
    if err != nil {
        return 0, fmt.Errorf("cleanup query: %w", err)
    }
    var ids []vector.GenerationID
    for rows.Next() {
        var id int64
        if err := rows.Scan(&id); err != nil {
            _ = rows.Close()
            return 0, err
        }
        ids = append(ids, vector.GenerationID(id))
    }
    if err := rows.Close(); err != nil {
        return 0, err
    }
    n := 0
    for _, id := range ids {
        if err := bb.DropGeneration(ctx, id); err != nil {
            return n, fmt.Errorf("drop %d: %w", id, err)
        }
        n++
    }
    return n, nil
}
```

- [ ] **Step 3: Tests PASS.**
- [ ] **Step 4: Commit**

```bash
git add internal/vector/embed/bench/cleanup.go internal/vector/embed/bench/cleanup_test.go
git commit -m "bench: drop orphan bench: generations on next invocation"
```

---

## Task 13: Stratified sampler

**Files:**
- Modify: `internal/vector/embed/bench/sample.go`
- Modify: `internal/vector/embed/bench/sample_test.go`

- [ ] **Step 1: Failing test**

Cover: stratification math (250/500/250 from 1000 candidates with 25/50/25 spec), seed reproducibility (same seed → same IDs), empty-stratum error (`source_type=imessage` against email-only main DB), `length` bucketing measured post-preprocess, deterministic across runs.

- [ ] **Step 2: Implement**

```go
package bench

import (
    "context"
    "database/sql"
    "encoding/json"
    "fmt"
    "math/rand"
    "sort"
    "strings"

    "github.com/wesm/msgvault/internal/mime"
    "github.com/wesm/msgvault/internal/vector/embed"
)

// StratifySpec describes the stratification request: which axes to
// stratify on and the per-bucket weights.
type StratifySpec struct {
    Length      map[string]float64 `json:"length,omitempty"`      // short/medium/long → weight (0..1)
    Year        map[string]float64 `json:"year,omitempty"`        // "2014" → weight
    Account     map[string]float64 `json:"account,omitempty"`     // sources.identifier → weight
    Attachments map[string]float64 `json:"attachments,omitempty"` // with/without → weight
    SourceType  map[string]float64 `json:"source_type,omitempty"` // sources.kind → weight
}

// CreateStratifiedSample builds a sample by reading the candidate set
// from mainDB, bucketing each candidate per the spec, and selecting
// floor(weight*size) (with remainder distribution) from each non-
// empty bucket. Errors on any bucket whose request count > available.
func CreateStratifiedSample(ctx context.Context, vecDB, mainDB *sql.DB, name string, size int, spec StratifySpec, seed int64, pp embed.PreprocessConfig, maxInputChars int, notes string) error {
    // 1. Pull candidate set (id, sent_at year, source_id, source_kind, source_identifier, has_attachments).
    rows, err := mainDB.QueryContext(ctx, `
        SELECT m.id,
               COALESCE(strftime('%Y', m.sent_at), '') AS year,
               COALESCE(s.kind, '') AS source_kind,
               COALESCE(s.identifier, '') AS source_identifier,
               EXISTS (SELECT 1 FROM attachments a WHERE a.message_id = m.id) AS has_attachment,
               COALESCE(m.subject, ''),
               COALESCE(mb.body_text, ''),
               COALESCE(mb.body_html, '')
          FROM messages m
          LEFT JOIN sources s ON s.id = m.source_id
          LEFT JOIN message_bodies mb ON mb.message_id = m.id
         WHERE m.deleted_from_source_at IS NULL`)
    if err != nil {
        return fmt.Errorf("CreateStratifiedSample: candidate query: %w", err)
    }
    defer func() { _ = rows.Close() }()

    type cand struct {
        ID           int64
        Year         string
        Kind         string
        Identifier   string
        HasAttach    bool
        LengthBucket string
    }
    var cands []cand
    for rows.Next() {
        var c cand
        var subject, bodyText, bodyHTML string
        if err := rows.Scan(&c.ID, &c.Year, &c.Kind, &c.Identifier, &c.HasAttach, &subject, &bodyText, &bodyHTML); err != nil {
            return err
        }
        body := bodyText
        if body == "" && bodyHTML != "" {
            body = mime.StripHTML(bodyHTML)
        }
        text, _ := embed.Preprocess(subject, body, maxInputChars, pp)
        chars := len([]rune(text))
        switch {
        case chars < 500:
            c.LengthBucket = "short"
        case chars < 5000:
            c.LengthBucket = "medium"
        default:
            c.LengthBucket = "long"
        }
        cands = append(cands, c)
    }
    if err := rows.Err(); err != nil {
        return err
    }

    // 2. For each enabled axis, partition candidates into buckets and
    //    pick floor(w*size) (+ remainder distribution) from each.
    rng := rand.New(rand.NewSource(seed))
    pick := func(bucket []cand, target int) []cand {
        if len(bucket) <= target {
            return bucket
        }
        rng.Shuffle(len(bucket), func(i, j int) { bucket[i], bucket[j] = bucket[j], bucket[i] })
        return bucket[:target]
    }
    type bucketDef struct {
        Axis   string
        Key    string
        Weight float64
        Match  func(cand) bool
    }
    var defs []bucketDef
    addBuckets := func(axis string, weights map[string]float64, match func(cand, string) bool) {
        for k, w := range weights {
            kk := k
            defs = append(defs, bucketDef{Axis: axis, Key: kk, Weight: w, Match: func(c cand) bool { return match(c, kk) }})
        }
    }
    if len(spec.Length) > 0 {
        addBuckets("length", spec.Length, func(c cand, k string) bool { return c.LengthBucket == k })
    }
    if len(spec.Year) > 0 {
        addBuckets("year", spec.Year, func(c cand, k string) bool { return c.Year == k })
    }
    if len(spec.Account) > 0 {
        addBuckets("account", spec.Account, func(c cand, k string) bool { return c.Identifier == k })
    }
    if len(spec.Attachments) > 0 {
        addBuckets("attachments", spec.Attachments, func(c cand, k string) bool {
            switch k {
            case "with":
                return c.HasAttach
            case "without":
                return !c.HasAttach
            }
            return false
        })
    }
    if len(spec.SourceType) > 0 {
        addBuckets("source_type", spec.SourceType, func(c cand, k string) bool { return c.Kind == k })
    }

    if len(defs) == 0 {
        // Unstratified: pick `size` uniformly.
        if len(cands) == 0 {
            return fmt.Errorf("CreateStratifiedSample: no candidate messages")
        }
        ids := make([]int64, len(cands))
        for i, c := range cands {
            ids[i] = c.ID
        }
        rng.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
        if size > len(ids) {
            size = len(ids)
        }
        ids = ids[:size]
        sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
        return CreateSampleFromIDs(ctx, vecDB, name, ids, SampleMeta{Seed: seed, Notes: notes})
    }

    // Allocate per-bucket targets via floor + remainder distribution.
    type alloc struct {
        Def    bucketDef
        Bucket []cand
        Target int
        Frac   float64
    }
    allocs := make([]alloc, 0, len(defs))
    totalFloor := 0
    for _, d := range defs {
        var bucket []cand
        for _, c := range cands {
            if d.Match(c) {
                bucket = append(bucket, c)
            }
        }
        target := int(float64(size) * d.Weight)
        frac := float64(size)*d.Weight - float64(target)
        allocs = append(allocs, alloc{Def: d, Bucket: bucket, Target: target, Frac: frac})
        totalFloor += target
    }
    // Distribute remainder by descending Frac.
    remainder := size - totalFloor
    sort.Slice(allocs, func(i, j int) bool { return allocs[i].Frac > allocs[j].Frac })
    for i := 0; remainder > 0 && i < len(allocs); i++ {
        allocs[i].Target++
        remainder--
    }
    var picked []int64
    var stratums []string
    for _, a := range allocs {
        if a.Target == 0 {
            continue
        }
        if len(a.Bucket) == 0 {
            return fmt.Errorf("stratum %q=%q: 0 candidate messages — these source types are not currently in the embedding corpus", a.Def.Axis, a.Def.Key)
        }
        if len(a.Bucket) < a.Target {
            return fmt.Errorf("stratum %q=%q: only %d candidates available, requested %d", a.Def.Axis, a.Def.Key, len(a.Bucket), a.Target)
        }
        chosen := pick(a.Bucket, a.Target)
        for _, c := range chosen {
            picked = append(picked, c.ID)
            stratums = append(stratums, fmt.Sprintf("%s=%s", a.Def.Axis, a.Def.Key))
        }
    }
    // Sort by ID for deterministic insertion order.
    type pair struct {
        ID      int64
        Stratum string
    }
    pairs := make([]pair, len(picked))
    for i := range picked {
        pairs[i] = pair{ID: picked[i], Stratum: stratums[i]}
    }
    sort.Slice(pairs, func(i, j int) bool { return pairs[i].ID < pairs[j].ID })
    finalIDs := make([]int64, len(pairs))
    finalStrata := make([]string, len(pairs))
    for i, p := range pairs {
        finalIDs[i] = p.ID
        finalStrata[i] = p.Stratum
    }
    specJSON, _ := json.Marshal(spec)
    return CreateSampleFromIDs(ctx, vecDB, name, finalIDs, SampleMeta{Seed: seed, StratifySpec: string(specJSON), Notes: notes}, finalStrata)
}

// ParseStratifySpec parses a CLI -stratify string into a StratifySpec.
// Format: `dim=v1[:wpct],v2[:wpct]` separated by `,`. Per-axis weights
// must sum to 1.0 (within 0.01); equal weighting is implied when no
// :pct is given.
func ParseStratifySpec(s string) (StratifySpec, error) {
    var spec StratifySpec
    if s == "" {
        return spec, nil
    }
    // Split into axis groups by `;` (allow `,` only for value lists per axis).
    // Format: axis=v1:25%,v2:50%,v3:25%;axis2=...
    axisGroups := strings.Split(s, ";")
    for _, g := range axisGroups {
        eq := strings.IndexByte(g, '=')
        if eq < 0 {
            return spec, fmt.Errorf("stratify: %q: missing '='", g)
        }
        axis := strings.TrimSpace(g[:eq])
        list := g[eq+1:]
        weights, err := parseWeightList(list)
        if err != nil {
            return spec, fmt.Errorf("stratify axis %q: %w", axis, err)
        }
        switch axis {
        case "length":
            spec.Length = weights
        case "year":
            spec.Year = weights
        case "account":
            spec.Account = weights
        case "attachments":
            spec.Attachments = weights
        case "source_type":
            spec.SourceType = weights
        default:
            return spec, fmt.Errorf("stratify: unknown axis %q (valid: length, year, account, attachments, source_type)", axis)
        }
    }
    return spec, nil
}

func parseWeightList(s string) (map[string]float64, error) {
    parts := strings.Split(s, ",")
    weights := make(map[string]float64, len(parts))
    var withPct, withoutPct []string
    for _, p := range parts {
        p = strings.TrimSpace(p)
        if p == "" {
            continue
        }
        colon := strings.IndexByte(p, ':')
        if colon < 0 {
            withoutPct = append(withoutPct, p)
            continue
        }
        key := strings.TrimSpace(p[:colon])
        wStr := strings.TrimSpace(p[colon+1:])
        wStr = strings.TrimSuffix(wStr, "%")
        var pct float64
        if _, err := fmt.Sscanf(wStr, "%f", &pct); err != nil {
            return nil, fmt.Errorf("weight %q: %w", p, err)
        }
        weights[key] = pct / 100.0
        withPct = append(withPct, key)
    }
    if len(withPct) > 0 && len(withoutPct) > 0 {
        return nil, fmt.Errorf("mix of explicit and implicit weights not allowed; use one consistent style")
    }
    if len(withoutPct) > 0 {
        share := 1.0 / float64(len(withoutPct))
        for _, k := range withoutPct {
            weights[k] = share
        }
    }
    var total float64
    for _, w := range weights {
        total += w
    }
    if total < 0.99 || total > 1.01 {
        return nil, fmt.Errorf("weights sum to %.2f, expected 1.0", total)
    }
    return weights, nil
}
```

- [ ] **Step 3: Tests PASS.**
- [ ] **Step 4: Commit**

```bash
git add internal/vector/embed/bench/sample.go internal/vector/embed/bench/sample_test.go
git commit -m "bench: stratified sampler with seed reproducibility and empty-stratum guard"
```

---

## Task 14: Run / sweep orchestrators

**Files:**
- Create: `internal/vector/embed/bench/run.go`
- Create: `internal/vector/embed/bench/run_test.go`

- [ ] **Step 1: Failing tests** — covering at minimum:
  - `RunCell` (endpoint mode) against a fake `EmbedClient` persists a `bench_runs` row with the correct `cell_json`, `config_hash`, `msgs_succeeded`, and `status='completed'`.
  - `RunCell` (endpoint mode) where ctx is cancelled mid-flight persists a row with `status='aborted'` and a non-empty `error_message`.
  - `RunCell` (pipeline mode) persists `bench_runs.generation_id` with the gen ID returned by `RunPipeline`.
  - `RunSweep` over a 2-cell matrix persists one `bench_sweeps` row + 2 `bench_runs` rows linked by `sweep_id`. Each `bench_runs.cell_json` reflects the cell's coordinates.
  - `RunSweep` writes the auto-plan TOML to `<tmpDir>/sweeps/auto/<UTC>.toml` when given a synthesized plan (override the default path via a test-only `AutoPlanDir` field on the input struct).

- [ ] **Step 2: Implement orchestrator**

Function shape:

```go
// RunCellInputs bundles the per-cell parameters needed for one row in
// bench_runs.
type RunCellInputs struct {
    SweepID      *int64        // nil for ad-hoc `run`
    SampleName   string
    SampleIDs    []int64       // pre-loaded from bench_sample_messages
    Mode         string        // "endpoint" | "pipeline"
    Cell         Cell          // matrix coordinates
    ResolvedConfig map[string]any // canonical config (model, dim, batch_size, workers, max_input_chars, preprocess, ...)
    BackendBase  vector.Backend
    MainDB       *sql.DB
    VectorsDB    *sql.DB
    Client       embed.EmbeddingClient   // pre-built from cell.endpoint
    Preprocess   embed.PreprocessConfig
    MaxInputChars int
    BatchSize    int
    Workers      int
    WarmupBatches int
}

// RunCell executes one matrix cell and persists exactly one
// bench_runs row. Returns the inserted row id and a non-nil error
// only when the row could not be persisted; per-batch failures are
// recorded in the row's error counters and `status='completed'`.
func RunCell(ctx context.Context, in RunCellInputs) (runID int64, err error)
```

Status assignment rules:

| Outcome | `status` | `error_message` | DropGeneration (pipeline) |
|---------|----------|-----------------|---------------------------|
| All batches finish (any per-batch errors recorded in counters) | `completed` | NULL | called |
| `ctx.Err() == context.Canceled` mid-run | `aborted` | `"context canceled"` | called via `RunPipeline`'s defer |
| `ctx.Err() == context.DeadlineExceeded` mid-run | `aborted` | `"context deadline exceeded"` | called |
| Backend type-assert fails (pipeline mode) | `error` | the error message | n/a (no gen created) |
| `RunPipeline` returns non-cancel error | `error` | the error message | called via the same defer |
| `bench_runs` insert itself fails after a successful pipeline run | the function returns the error to the caller; the gen has already been dropped by `RunPipeline`'s defer, so no orphan | n/a | called |

In endpoint mode, error counters are aggregated by `Aggregator` and end up in `errors_4xx`/`errors_5xx`/`errors_429`/`errors_network` — these never set `status='error'`. Only ctx errors and backend-level errors do.

Pipeline-mode serialization: the `state='building'` unique partial index in `index_generations` allows only one building generation at a time. **The sweep orchestrator MUST run pipeline-mode cells strictly sequentially** (one at a time across the whole sweep). Endpoint-mode cells can run concurrently with each other and with at most one pipeline-mode cell. The simplest and safest implementation: serialize *all* cells in a sweep within a single goroutine. If concurrency is desired later, gate it behind an explicit `parallel_endpoint_cells` flag.

`bench_runs.generation_id` is populated from `PipelineResult.GenerationID` (NULL in endpoint mode).

Preprocessing happens *only in endpoint mode* — RunCell loads sample IDs, fetches subjects/bodies from main DB, runs `embed.Preprocess`, drops empties (counted as `msgs_dropped`), and feeds `PreparedMessage`s into `RunEndpoint`. **Pipeline mode skips this**: the existing `embed.Worker.embedBatch` already performs the same fetch+preprocess inside `RunOnce`, and double-preprocessing would double-count work. Pipeline mode hands raw sample IDs to `RunPipeline` which inserts them into `pending_embeddings` directly; the worker does the rest.

Auto-plan path: `os.MkdirAll(filepath.Join(home, ".msgvault", "sweeps", "auto"), 0o755)` before write.

Function size: keep `RunCell` ≤80 lines by extracting `loadAndPreprocess`, `buildClient`, `persistRunRow` helpers.

- [ ] **Step 3: Tests PASS** — including the sequential-pipeline invariant: a sweep with two pipeline cells executes them one at a time.

- [ ] **Step 4: Commit**

```bash
git add internal/vector/embed/bench/run.go internal/vector/embed/bench/run_test.go
git commit -m "bench: orchestrate RunCell/RunSweep with status rules and pipeline serialization"
```

---

## Task 15: Reports — list / show / compare

**Files:**
- Create: `internal/vector/embed/bench/report.go`
- Create: `internal/vector/embed/bench/report_test.go`

- [ ] **Step 1: Failing tests** — `RenderShow(run)` produces the documented terminal table (snapshot test on a fixture run); `RenderCompare(runs)` highlights ≥5% deltas; `--json` paths emit valid JSON matching the schema.
- [ ] **Step 2: Implement** — pure formatting; no DB writes. Use `text/tabwriter` for terminal alignment.
- [ ] **Step 3: PASS.**
- [ ] **Step 4: Commit**

```bash
git add internal/vector/embed/bench/report.go internal/vector/embed/bench/report_test.go
git commit -m "bench: add show/list/compare renderers with JSON output"
```

---

## Task 16: `embedshootout` CLI dispatcher

**Files:**
- Create: `scripts/embedshootout/main.go`

- [ ] **Step 1: Verb-style dispatcher**

Mirror `mimeshootout`'s style. First positional arg is the verb. Each verb owns a `*flag.FlagSet`. Flags `-vectors-db`, `-main-db` default to `$MSGVAULT_HOME/vectors.db` (resp. `msgvault.db`), falling back to `~/.msgvault/`. The dispatcher:

```go
func main() {
    if len(os.Args) < 2 {
        usage(); os.Exit(2)
    }
    verb := os.Args[1]
    args := os.Args[2:]
    var err error
    switch verb {
    case "sample-create":     err = cmdSampleCreate(args)
    case "sample-list":       err = cmdSampleList(args)
    case "sample-show":       err = cmdSampleShow(args)
    case "sample-delete":     err = cmdSampleDelete(args)
    case "run":               err = cmdRun(args)
    case "sweep":             err = cmdSweep(args)
    case "list":              err = cmdList(args)
    case "show":              err = cmdShow(args)
    case "compare":           err = cmdCompare(args)
    case "delete":            err = cmdDelete(args)
    case "-h", "--help", "help":
        usage(); return
    default:
        fmt.Fprintf(os.Stderr, "unknown verb %q\n", verb); usage(); os.Exit(2)
    }
    if err != nil {
        fmt.Fprintf(os.Stderr, "error: %v\n", err); os.Exit(1)
    }
}
```

Each `cmdX` function: parse its flags, open `vectors.db` and `msgvault.db`, and dispatch into the `bench` package.

Concrete backend acquisition (pipeline-mode write verbs need `BenchBackend`):

```go
import (
    "github.com/wesm/msgvault/internal/vector"
    "github.com/wesm/msgvault/internal/vector/sqlitevec"
)

// openBackend returns a *sqlitevec.Backend (which implements
// vector.BenchBackend; build tag sqlite_vec is required).
func openBackend(ctx context.Context, vectorsPath, mainPath string, mainDB *sql.DB, dim int) (*sqlitevec.Backend, error) {
    return sqlitevec.Open(ctx, sqlitevec.Options{
        Path:      vectorsPath,
        MainPath:  mainPath,
        Dimension: dim,
        MainDB:    mainDB,
    })
}

// asBenchBackend wraps the type assertion with a clear error if the
// backend doesn't implement the capability (today, all sqlitevec
// backends do, but build-tag-stripped builds may not).
func asBenchBackend(b vector.Backend) (vector.BenchBackend, error) {
    bb, ok := b.(vector.BenchBackend)
    if !ok {
        return nil, fmt.Errorf("backend does not implement BenchBackend (rebuild with -tags sqlite_vec)")
    }
    return bb, nil
}
```

Open `vectors.db` for write verbs with lazy `bench.EnsureSchema`; for read verbs (`sample-list`, `list`, `show`, `compare`) call `bench.RequireSchema` and exit cleanly with `bench.ErrNoBenchData` mapped to a "no bench data yet — create a sample first" message.

At the start of any write verb (`sample-create`, `run`, `sweep`, `delete`), call `bench.CleanupOrphanGenerations(ctx, db, bb, time.Hour)` opportunistically. Log the count of dropped gens at info level if non-zero; do not fail the verb on cleanup errors (the verb's primary work is more important than tidying orphans).

- [ ] **Step 2: Build the binary**

`go build -tags "fts5 sqlite_vec" -o embedshootout ./scripts/embedshootout`

Expected: produces `./embedshootout` at the repo root.

- [ ] **Step 3: Smoke `--help`**

`./embedshootout help` prints the verb list. Exit code 0.

- [ ] **Step 4: Commit**

```bash
git add scripts/embedshootout/main.go
git commit -m "embedshootout: stdlib flag verb dispatcher over bench package"
```

---

## Task 17: Makefile targets

**Files:**
- Modify: `Makefile`

- [ ] **Step 1: Add targets**

```makefile
.PHONY: ... embedshootout run-embedshootout

# Build the embedding shootout tool
embedshootout:
	CGO_ENABLED=1 go build -tags "$(BUILD_TAGS)" -o embedshootout ./scripts/embedshootout

# Convenience smoke pass (requires a sample named 'default')
run-embedshootout: embedshootout
	./embedshootout run -mode endpoint -sample default -batch-size 32 -workers 1
```

Update `clean` to `rm -f msgvault msgvault.exe mimeshootout embedshootout`.

Add `embedshootout` and `run-embedshootout` lines to the `help` target.

- [ ] **Step 2: Verify**

```bash
make embedshootout
ls -la ./embedshootout    # binary present
make help | grep -E "embedshootout|run-embedshootout"
```

- [ ] **Step 3: Commit**

```bash
git add Makefile
git commit -m "Makefile: add embedshootout and run-embedshootout targets"
```

---

## Task 18: End-to-end smoke

**Files:** none — this task is purely verification.

- [ ] **Step 1: Run the test suite under both build tags**

```bash
go test -tags "fts5 sqlite_vec" ./...
```

Expected: PASS, including all new bench tests and unchanged existing tests.

- [ ] **Step 2: Run `go vet` and `gofmt`**

```bash
gofmt -l ./...           # expected: empty output
go vet -tags "fts5 sqlite_vec" ./...   # expected: no issues
```

- [ ] **Step 3: Build embedshootout against a real-shaped (in-memory) corpus** — fire up a tiny HTTP fake (the existing `embed.Client` test infra has `httptest.NewServer` patterns) and run a 1-cell endpoint sweep against a synthetic 50-message sample. Verify a `bench_runs` row lands with sane numbers.

- [ ] **Step 4: Commit (no code changes; if any docs were added during smoke, commit them)**

```bash
# usually a no-op commit step
git status
```

---

## Task 19: Developer doc

**Files:**
- Create: `docs/embedshootout.md` (one page)

- [ ] **Step 1: Write a one-page guide** — install (`make embedshootout`), create a sample, run a sweep, read the output, troubleshoot. Link to the spec.
- [ ] **Step 2: Add a single-line entry to CLAUDE.md under "Quick Commands" linking to the doc.** (per CLAUDE.md "Documentation" section in the spec)
- [ ] **Step 3: Commit**

```bash
git add docs/embedshootout.md CLAUDE.md
git commit -m "docs: add embedshootout developer guide"
```

---

## Notes for the executing engineer

- Run `gofmt -w ./...` and `go vet -tags "fts5 sqlite_vec" ./...` at the end of every task before committing — repo policy in CLAUDE.md.
- Never leave a task half-implemented at commit time — each task's commit must build and pass tests on its own.
- `embed.ErrPermanent4xx` is already in `internal/vector/embed/client.go`; reference it directly.
- The shared `rateWindow` helper is *not* needed for embedshootout itself — bench progress is event-driven and aggregates land in DB. Any "live progress line" the CLI prints is decorative; if it imports `rateWindow`, follow whatever location the windowed-ETA spec settles on.
- Foreign-key cascade requires `PRAGMA foreign_keys = ON` per connection. The standard `sqlitevec.Open` path enables it via the `RegisterExtension` ConnectHook. In tests that open `:memory:` directly, set the pragma manually (see Task 6 `TestDeleteSample_Cascade`).
- Keep individual `bench/*.go` files under ~300 lines. If a file grows past that, split by responsibility before committing.
