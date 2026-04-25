# Embed Worker: Windowed ETA + Oversize-Batch Downshift Implementation Plan

**Goal:** Replace the embed worker's cumulative-rate ETA with a sliding-window average over the last N batches (default 10, TOML-configurable), and add a localized batch-size downshift to drain oversize batches one message at a time when the embedding endpoint returns a non-retryable 4xx.

**Architecture:** Two independent changes share one worktree because they touch the same files.

1. ETA smoothing lives **in the printer** (`cmd/msgvault/cmd/embed_vector.go`), not in the worker. The worker keeps emitting one `ProgressReport` per successful batch with `BatchMsgs` and `BatchElapsed`; the printer maintains a ring buffer and computes `sum(msgs)/sum(elapsed)` over the last N entries. A new `ETAWindow` field in `EmbeddingsConfig` (default 10) controls N.
2. Downshift behavior lives **in the worker** (`internal/vector/embed/worker.go`). A new `embed.ErrPermanent4xx` sentinel from the embed client lets `errors.Is` detect non-retryable 4xx; on a multi-message batch the worker calls a new `downshiftDrain` that walks the same already-claimed IDs one at a time, embedding survivors and dropping singleton 4xxs (same pattern as the existing empty-input drain). Counter rules preserve the abort cap when every singleton drops, so a fully misconfigured endpoint still surfaces the original 4xx body.

**Tech Stack:** Go 1.22+, SQLite (vec extension via `sqlite_vec` build tag), existing `embed.Client` retry loop, existing `Worker.RunOnce` batch loop, `internal/vector/config.go` TOML defaults.

**Spec:** [`docs/specs/2026-04-25-embed-windowed-eta-and-batch-downshift-design.md`](../specs/2026-04-25-embed-windowed-eta-and-batch-downshift-design.md)

---

## File Map

| File | Responsibility | Action |
|------|----------------|--------|
| `internal/vector/config.go` | Holds `EmbeddingsConfig.ETAWindow`; defaulted to 10 in `ApplyDefaults` | Modify |
| `internal/vector/config_test.go` | Default + zero-value test for ETAWindow | Modify |
| `internal/vector/embed/client.go` | New `ErrPermanent4xx` sentinel; 4xx branch wraps with `%w` | Modify |
| `internal/vector/embed/client_test.go` | `errors.Is(err, ErrPermanent4xx)` for 4xx vs. 5xx/429/network | Modify |
| `internal/vector/embed/worker.go` | New `downshiftDrain` method; integration into `RunOnce` failure branch; updated counter rules | Modify |
| `internal/vector/embed/worker_test.go` | End-to-end downshift cases (success, partial drop, all drop, ctx cancel mid-drain, singleton 4xx outside drain) | Modify |
| `cmd/msgvault/cmd/embed_progress.go` | New file (no build tag): `rateWindow` ring buffer type | Create |
| `cmd/msgvault/cmd/embed_progress_test.go` | New file (no build tag): `rateWindow` table-driven tests | Create |
| `cmd/msgvault/cmd/embed_vector.go` | `newProgressPrinter` takes `windowSize`; uses `rateWindow`; new line format with `(last K)` annotation; `runEmbed` passes `cfg.Vector.Embeddings.ETAWindow` | Modify |
| `cmd/msgvault/cmd/embed_vector_test.go` | Printer integration test feeding synthetic `ProgressReport`s, asserting windowed-rate output | Modify |

The `embed_progress.go` split from `embed_vector.go` is deliberate: `embed_vector.go` is gated by `//go:build sqlite_vec`, but the ring buffer has zero sqlite_vec dependency. Keeping it in an untagged file means its tests run on every platform/build, including CI runs that don't enable the tag.

---

## Task 1: Add `ETAWindow` config field

**Files:**
- Modify: `internal/vector/config.go:27-36` (struct), `:139-172` (ApplyDefaults)
- Modify: `internal/vector/config_test.go`

- [ ] **Step 1: Write the failing default test**

In `internal/vector/config_test.go`, add (or extend an existing defaults test if one exists — search for `ApplyDefaults` first):

```go
func TestEmbeddingsConfig_ETAWindowDefault(t *testing.T) {
    var c Config
    c.Embeddings.Endpoint = "http://localhost:1234/v1"
    c.ApplyDefaults()
    if c.Embeddings.ETAWindow != 10 {
        t.Fatalf("ETAWindow default: got %d, want 10", c.Embeddings.ETAWindow)
    }
}

func TestEmbeddingsConfig_ETAWindowExplicit(t *testing.T) {
    var c Config
    c.Embeddings.Endpoint = "http://localhost:1234/v1"
    c.Embeddings.ETAWindow = 25
    c.ApplyDefaults()
    if c.Embeddings.ETAWindow != 25 {
        t.Fatalf("ETAWindow explicit: got %d, want 25", c.Embeddings.ETAWindow)
    }
}
```

- [ ] **Step 2: Run the test and confirm it fails**

```bash
go test ./internal/vector/ -run TestEmbeddingsConfig_ETAWindow -v
```

Expected: FAIL with "c.Embeddings.ETAWindow undefined".

- [ ] **Step 3: Add the field and the default**

In `internal/vector/config.go`, extend `EmbeddingsConfig`:

```go
type EmbeddingsConfig struct {
    Endpoint      string        `toml:"endpoint"`
    // ... existing fields unchanged ...
    BatchSize     int           `toml:"batch_size"`
    Timeout       time.Duration `toml:"timeout"`
    MaxRetries    int           `toml:"max_retries"`
    MaxInputChars int           `toml:"max_input_chars"`
    ETAWindow     int           `toml:"eta_window"`
}
```

In `ApplyDefaults` (around line 152, next to the `MaxInputChars` default):

```go
if c.Embeddings.ETAWindow <= 0 {
    c.Embeddings.ETAWindow = 10
}
```

Note `<= 0` (not `== 0`): negative values in TOML are also normalized to the default rather than letting them slip through.

- [ ] **Step 4: Run the test and confirm it passes**

```bash
go test ./internal/vector/ -run TestEmbeddingsConfig_ETAWindow -v
```

Expected: PASS for both subtests.

- [ ] **Step 5: Run all vector config tests to confirm nothing else broke**

```bash
go test ./internal/vector/ -v
```

Expected: all tests PASS.

- [ ] **Step 6: Commit**

```bash
go fmt ./...
go vet ./...
git add internal/vector/config.go internal/vector/config_test.go
git commit -m "embed config: add eta_window with default 10"
```

---

## Task 2: Build the `rateWindow` ring buffer

**Files:**
- Create: `cmd/msgvault/cmd/embed_progress.go`
- Create: `cmd/msgvault/cmd/embed_progress_test.go`

- [ ] **Step 1: Write the failing tests**

Create `cmd/msgvault/cmd/embed_progress_test.go`:

```go
package cmd

import (
    "math"
    "testing"
    "time"
)

func TestRateWindow_EmptyReturnsZero(t *testing.T) {
    w := newRateWindow(10)
    if r := w.Rate(); r != 0 {
        t.Fatalf("empty Rate: got %v, want 0", r)
    }
    if n := w.Samples(); n != 0 {
        t.Fatalf("empty Samples: got %d, want 0", n)
    }
}

func TestRateWindow_PartialFill(t *testing.T) {
    w := newRateWindow(10)
    w.Add(50, 1*time.Second)
    w.Add(100, 1*time.Second)
    if got, want := w.Samples(), 2; got != want {
        t.Fatalf("Samples: got %d, want %d", got, want)
    }
    // sum(msgs)/sum(seconds) = 150/2 = 75
    if r := w.Rate(); math.Abs(r-75) > 0.01 {
        t.Fatalf("Rate: got %v, want ~75", r)
    }
}

func TestRateWindow_EvictsOldestOnceFull(t *testing.T) {
    w := newRateWindow(3)
    // Fill with low-rate samples.
    w.Add(10, 1*time.Second) // 10 msg/s
    w.Add(10, 1*time.Second)
    w.Add(10, 1*time.Second)
    if r := w.Rate(); math.Abs(r-10) > 0.01 {
        t.Fatalf("pre-eviction Rate: got %v, want 10", r)
    }
    // Push a high-rate sample; oldest 10/1 should fall out.
    w.Add(1000, 1*time.Second)
    // Now window holds three samples: 10, 10, 1000 over 3s -> 340 msg/s.
    if got, want := w.Samples(), 3; got != want {
        t.Fatalf("Samples after eviction: got %d, want %d", got, want)
    }
    if r := w.Rate(); math.Abs(r-340) > 0.01 {
        t.Fatalf("post-eviction Rate: got %v, want 340", r)
    }
}

func TestRateWindow_WeightedNotMeanOfPerBatchRates(t *testing.T) {
    // 1 msg in 1s would be 1 msg/s; 99 msgs in 1s would be 99 msg/s.
    // Mean of per-batch rates = 50. Weighted = (1+99)/2 = 50 here, so
    // pick numbers that distinguish the two.
    //   Batch A: 1 msg in 1s   -> 1 msg/s
    //   Batch B: 100 msgs in 1s -> 100 msg/s
    //   Mean of rates: 50.5
    //   Weighted: 101 / 2 = 50.5  -- still equal; use different elapseds.
    //   Batch A: 1 msg in 10s   -> 0.1 msg/s
    //   Batch B: 100 msgs in 1s -> 100 msg/s
    //   Mean of rates: 50.05
    //   Weighted: 101 / 11 = 9.18  -- distinguishes.
    w := newRateWindow(2)
    w.Add(1, 10*time.Second)
    w.Add(100, 1*time.Second)
    got := w.Rate()
    want := 101.0 / 11.0
    if math.Abs(got-want) > 0.01 {
        t.Fatalf("weighted Rate: got %v, want ~%v (must NOT equal mean-of-rates ~50)", got, want)
    }
}

func TestRateWindow_ZeroElapsedSampleSkipped(t *testing.T) {
    w := newRateWindow(10)
    w.Add(10, 1*time.Second)
    w.Add(50, 0)               // skipped: would divide by zero
    w.Add(20, 1*time.Second)
    if got, want := w.Samples(), 2; got != want {
        t.Fatalf("Samples: got %d (zero-elapsed should be skipped), want %d", got, want)
    }
    // 30 msgs over 2s = 15.
    if r := w.Rate(); math.Abs(r-15) > 0.01 {
        t.Fatalf("Rate: got %v, want 15", r)
    }
}

func TestRateWindow_NegativeElapsedSampleSkipped(t *testing.T) {
    w := newRateWindow(10)
    w.Add(10, 1*time.Second)
    w.Add(50, -1*time.Second) // pathological: skip
    if got, want := w.Samples(), 1; got != want {
        t.Fatalf("Samples: got %d, want %d", got, want)
    }
}

func TestNewRateWindow_NormalizesNonPositiveCap(t *testing.T) {
    w := newRateWindow(0)
    // Should not panic on Add.
    w.Add(1, 1*time.Second)
    if got := w.Samples(); got < 1 {
        t.Fatalf("Samples after Add on zero-cap window: got %d, want >= 1", got)
    }
}
```

- [ ] **Step 2: Run the tests and confirm they fail to build**

```bash
go test ./cmd/msgvault/cmd/ -run TestRateWindow -v
```

Expected: BUILD FAIL ("undefined: newRateWindow").

- [ ] **Step 3: Implement `rateWindow`**

Create `cmd/msgvault/cmd/embed_progress.go`:

```go
package cmd

import "time"

// rateWindow accumulates per-batch (msgs, elapsed) samples in a ring
// buffer and reports the message-weighted rate over the window:
//
//     rate = sum(msgs) / sum(elapsed_seconds)
//
// Weighting matters when batch sizes vary a lot, e.g. when the worker
// downshifts to BatchSize=1 to drain a poison message: a simple mean
// of per-batch rates would let those tiny batches dominate the
// displayed throughput. Summing first keeps small batches'
// contribution proportional to their size.
type rateWindow struct {
    msgs    []int
    elapsed []time.Duration
    head    int // index of the next write
    count   int // number of valid entries (<= len(msgs))
}

// newRateWindow constructs a ring buffer of the given capacity. A
// non-positive cap is normalized to 1 so callers can always Add and
// Rate without checking — this matches how applyDefaults treats
// non-positive ETAWindow values from TOML, but defends against any
// caller that bypasses defaults.
func newRateWindow(cap int) *rateWindow {
    if cap < 1 {
        cap = 1
    }
    return &rateWindow{
        msgs:    make([]int, cap),
        elapsed: make([]time.Duration, cap),
    }
}

// Add records one batch sample. Samples with non-positive elapsed are
// silently skipped: they would either divide by zero (elapsed == 0)
// or contribute negative time (elapsed < 0, only reachable via clock
// non-monotonicity in test harnesses). The next legitimate sample
// re-establishes a healthy window.
func (w *rateWindow) Add(msgs int, elapsed time.Duration) {
    if elapsed <= 0 {
        return
    }
    w.msgs[w.head] = msgs
    w.elapsed[w.head] = elapsed
    w.head = (w.head + 1) % len(w.msgs)
    if w.count < len(w.msgs) {
        w.count++
    }
}

// Samples returns the number of valid entries currently in the
// window. Useful for the printer's "(last K)" annotation when the
// run hasn't yet emitted enough batches to fill the window.
func (w *rateWindow) Samples() int { return w.count }

// Rate returns the message-weighted rate in messages per second.
// Returns 0 when the window is empty so callers can treat 0 as "no
// estimate yet" and fall back to a no-ETA display branch.
func (w *rateWindow) Rate() float64 {
    if w.count == 0 {
        return 0
    }
    var totalMsgs int
    var totalElapsed time.Duration
    for i := 0; i < w.count; i++ {
        totalMsgs += w.msgs[i]
        totalElapsed += w.elapsed[i]
    }
    if totalElapsed <= 0 {
        return 0
    }
    return float64(totalMsgs) / totalElapsed.Seconds()
}
```

- [ ] **Step 4: Run the tests and confirm they pass**

```bash
go test ./cmd/msgvault/cmd/ -run TestRateWindow -v
```

Expected: all subtests PASS.

- [ ] **Step 5: Commit**

```bash
go fmt ./...
go vet ./...
git add cmd/msgvault/cmd/embed_progress.go cmd/msgvault/cmd/embed_progress_test.go
git commit -m "embed progress: add rateWindow ring buffer for sliding ETA"
```

---

## Task 3: Wire `rateWindow` into `newProgressPrinter`

**Files:**
- Modify: `cmd/msgvault/cmd/embed_vector.go:23-128` (`runEmbed`), `:261-296` (`newProgressPrinter`)
- Modify: `cmd/msgvault/cmd/embed_vector_test.go`

- [ ] **Step 1: Write the failing printer test**

In `cmd/msgvault/cmd/embed_vector_test.go`, add:

```go
func TestNewProgressPrinter_UsesWindowedRate(t *testing.T) {
    var buf bytes.Buffer
    // window=2, total=300 so the percent path runs.
    print := newProgressPrinter(&buf, 300, 2)

    // Force the first event past the throttle by seeding lastPrint
    // through a long enough simulated gap is awkward — easier: emit
    // the final event (Done >= total) which the printer always
    // flushes regardless of throttle.
    //
    // Two batches: 100 msgs in 1s, then 100 msgs in 1s. After the
    // second, Done=200 (not final). Then a 100-msg batch in 1s for
    // Done=300, which is final and bypasses throttle.
    print(embed.ProgressReport{
        Done: 100, TotalPending: 300,
        BatchMsgs: 100, BatchChars: 1000,
        BatchElapsed: 1 * time.Second,
        RunElapsed:   1 * time.Second,
    })
    // Drop the throttled-out output so the assertion below sees only
    // the final line. (Buffer carries it; assertion uses Contains.)
    print(embed.ProgressReport{
        Done: 200, TotalPending: 300,
        BatchMsgs: 100, BatchChars: 1000,
        BatchElapsed: 1 * time.Second,
        RunElapsed:   2 * time.Second,
    })
    print(embed.ProgressReport{
        Done: 300, TotalPending: 300,
        BatchMsgs: 100, BatchChars: 1000,
        BatchElapsed: 1 * time.Second,
        RunElapsed:   3 * time.Second,
    })

    out := buf.String()
    if !strings.Contains(out, "(last 2)") {
        t.Errorf("expected windowed-rate annotation `(last 2)`, got:\n%s", out)
    }
    // Window holds the last 2 batches: 200 msgs / 2s = 100 msg/s.
    if !strings.Contains(out, "100 msg/s") {
        t.Errorf("expected `100 msg/s` from windowed rate, got:\n%s", out)
    }
}
```

Add `bytes`, `strings`, `time`, and the existing `embed` import to the test file's imports if not already present.

- [ ] **Step 2: Run the test and confirm it fails**

```bash
go test -tags sqlite_vec ./cmd/msgvault/cmd/ -run TestNewProgressPrinter_UsesWindowedRate -v
```

Expected: FAIL — either compile error (signature mismatch) or assertion failure (annotation absent).

- [ ] **Step 3: Update `newProgressPrinter` and `runEmbed`**

In `cmd/msgvault/cmd/embed_vector.go`:

Change the printer signature from `newProgressPrinter(w io.Writer, total int)` to `newProgressPrinter(w io.Writer, total int, windowSize int)`. Replace its body with:

```go
func newProgressPrinter(w io.Writer, total int, windowSize int) func(embed.ProgressReport) {
    const minInterval = 2 * time.Second
    var lastPrint time.Time
    window := newRateWindow(windowSize)
    return func(p embed.ProgressReport) {
        now := time.Now()
        isFinal := total > 0 && p.Done >= total
        if !isFinal && now.Sub(lastPrint) < minInterval {
            return
        }
        lastPrint = now

        window.Add(p.BatchMsgs, p.BatchElapsed)
        windowedRate := window.Rate()
        samples := window.Samples()

        msPerMsg := float64(p.BatchElapsed.Milliseconds()) / float64(max1(p.BatchMsgs))
        usPerChar := float64(p.BatchElapsed.Microseconds()) / float64(max1(p.BatchChars))

        if total > 0 && windowedRate > 0 {
            remaining := total - p.Done
            if remaining < 0 {
                remaining = 0
            }
            eta := time.Duration(float64(remaining)/windowedRate) * time.Second
            pct := 100 * float64(p.Done) / float64(total)
            fmt.Fprintf(w,
                "progress: %d/%d (%.1f%%) — %.0f msg/s (last %d), %.1f ms/msg, %.2f µs/char, ETA %s\n",
                p.Done, total, pct, windowedRate, samples, msPerMsg, usPerChar, formatETA(eta))
        } else {
            fmt.Fprintf(w,
                "progress: %d embedded — %.0f msg/s (last %d), %.1f ms/msg, %.2f µs/char\n",
                p.Done, windowedRate, samples, msPerMsg, usPerChar)
        }
    }
}
```

Update the call site in `runEmbed` (around line 91):

```go
Progress: newProgressPrinter(os.Stderr, totalPending, cfg.Vector.Embeddings.ETAWindow),
```

- [ ] **Step 4: Run the test and confirm it passes**

```bash
go test -tags sqlite_vec ./cmd/msgvault/cmd/ -run TestNewProgressPrinter_UsesWindowedRate -v
```

Expected: PASS.

- [ ] **Step 5: Run the existing cmd tests with the build tag and confirm nothing else broke**

```bash
go test -tags sqlite_vec ./cmd/msgvault/cmd/ -v
```

Expected: all tests PASS.

- [ ] **Step 6: Commit**

```bash
go fmt ./...
go vet -tags sqlite_vec ./...
git add cmd/msgvault/cmd/embed_vector.go cmd/msgvault/cmd/embed_vector_test.go
git commit -m "embed: use windowed rate for ETA in progress printer"
```

---

## Task 4: Add `ErrPermanent4xx` sentinel to `embed.Client`

**Files:**
- Modify: `internal/vector/embed/client.go:160-170` (4xx branch)
- Modify: `internal/vector/embed/client_test.go`

- [ ] **Step 1: Write the failing tests**

In `internal/vector/embed/client_test.go`, add (placement near `TestClient_Embed_Does_Not_Retry_4xx` at line 131 keeps related tests together):

```go
func TestClient_Embed_4xxIsPermanent(t *testing.T) {
    srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        http.Error(w, `{"error":{"message":"Invalid input"}}`, http.StatusBadRequest)
    }))
    defer srv.Close()

    c := NewClient(Config{
        Endpoint: srv.URL, Model: "m", Dimension: 4, MaxRetries: 3,
    })
    _, err := c.Embed(context.Background(), []string{"hello"})
    if err == nil {
        t.Fatalf("expected error on 400")
    }
    if !errors.Is(err, ErrPermanent4xx) {
        t.Fatalf("expected errors.Is(err, ErrPermanent4xx), got %v", err)
    }
    // Existing contract: body must still be in the message.
    if !strings.Contains(err.Error(), "Invalid input") {
        t.Errorf("expected body in error, got %v", err)
    }
}

func TestClient_Embed_5xxNotPermanent(t *testing.T) {
    var attempts int
    srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        attempts++
        http.Error(w, "boom", http.StatusInternalServerError)
    }))
    defer srv.Close()

    c := NewClient(Config{
        Endpoint: srv.URL, Model: "m", Dimension: 4, MaxRetries: 2,
    })
    _, err := c.Embed(context.Background(), []string{"hello"})
    if err == nil {
        t.Fatalf("expected error after retries exhausted")
    }
    if errors.Is(err, ErrPermanent4xx) {
        t.Fatalf("5xx should NOT match ErrPermanent4xx, got %v", err)
    }
}

func TestClient_Embed_429NotPermanent(t *testing.T) {
    srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        w.Header().Set("Retry-After", "0")
        http.Error(w, "slow down", http.StatusTooManyRequests)
    }))
    defer srv.Close()

    c := NewClient(Config{
        Endpoint: srv.URL, Model: "m", Dimension: 4, MaxRetries: 2,
    })
    _, err := c.Embed(context.Background(), []string{"hello"})
    if err == nil {
        t.Fatalf("expected error after retries exhausted")
    }
    if errors.Is(err, ErrPermanent4xx) {
        t.Fatalf("429 should NOT match ErrPermanent4xx, got %v", err)
    }
}
```

Add `errors` to the test file's imports if not already present.

- [ ] **Step 2: Run the tests and confirm they fail**

```bash
go test ./internal/vector/embed/ -run "TestClient_Embed_4xxIsPermanent|TestClient_Embed_5xxNotPermanent|TestClient_Embed_429NotPermanent" -v
```

Expected: BUILD FAIL ("undefined: ErrPermanent4xx").

- [ ] **Step 3: Add the sentinel and wrap the 4xx branch**

In `internal/vector/embed/client.go`, add the sentinel near the top of the file (under the `Config` doc, before `type Config struct`):

```go
// ErrPermanent4xx marks a non-retryable HTTP 4xx response from the
// embeddings endpoint. Use errors.Is(err, ErrPermanent4xx) to detect
// it; the error message still carries the status code and a bounded
// response body. 429 (rate-limited) and 5xx are NOT wrapped — they
// flow through the retry loop as transient errors.
var ErrPermanent4xx = errors.New("embed: non-retryable 4xx response")
```

In `doOnce` (lines 160-170), wrap the returned error with `%w` of `ErrPermanent4xx`:

```go
if resp.StatusCode >= 400 {
    body, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
    if err != nil {
        return nil, fmt.Errorf("embed: HTTP %d (read error body: %v): %w",
            resp.StatusCode, err, ErrPermanent4xx)
    }
    msg := strings.TrimSpace(string(body))
    if msg == "" {
        return nil, fmt.Errorf("embed: HTTP %d: %w", resp.StatusCode, ErrPermanent4xx)
    }
    return nil, fmt.Errorf("embed: HTTP %d: %s: %w",
        resp.StatusCode, msg, ErrPermanent4xx)
}
```

The 429 and 5xx branches above this stay unchanged — they wrap with `*retryError`, not `ErrPermanent4xx`.

- [ ] **Step 4: Run the tests and confirm they pass**

```bash
go test ./internal/vector/embed/ -run "TestClient_Embed_4xxIsPermanent|TestClient_Embed_5xxNotPermanent|TestClient_Embed_429NotPermanent" -v
```

Expected: all PASS.

- [ ] **Step 5: Run all client tests to make sure existing behavior held**

```bash
go test ./internal/vector/embed/ -run TestClient -v
```

Expected: all PASS (the existing `TestClient_Embed_Does_Not_Retry_4xx` should still pass — it doesn't check the error type, only that retries don't happen).

- [ ] **Step 6: Commit**

```bash
go fmt ./...
go vet ./...
git add internal/vector/embed/client.go internal/vector/embed/client_test.go
git commit -m "embed client: surface non-retryable 4xx via ErrPermanent4xx sentinel"
```

---

## Task 5: Add `downshiftDrain` to the worker

**Files:**
- Modify: `internal/vector/embed/worker.go` (existing batch loop in `RunOnce` near line 200-355; add new method `downshiftDrain`)
- Modify: `internal/vector/embed/worker_test.go`

This task implements the inline split-to-1 drain. The behavior matrix from the spec is the source of truth for the counter rules:

| Situation                                              | Δ on `consecutiveFailures`           |
|--------------------------------------------------------|--------------------------------------|
| Multi-msg batch returns `ErrPermanent4xx`              | +1 (existing); downshift drain runs  |
| Drain embeds ≥ 1 message                               | reset to 0                           |
| Drain drops every message (all singletons 4xx)         | unchanged (preserve upstream +1)     |
| Drain hits a non-4xx error                             | +1 (treated like upsert/complete)    |
| Singleton batch returns `ErrPermanent4xx` outside drain | +1 (existing path)                  |

### Harness reality check (read before starting)

Before writing tests, confirm the actual harness in `internal/vector/embed/testsupport_test.go`:

- The fixture constructor is `newWorkerFixture(t, n int) *workerFixture` (NOT `newWorkerHarness`). It seeds `n` messages.
- The fixture's fields are `MainDB`, `VectorsDB`, `Backend`, `BuildingGen` (a `vector.GenerationID`), and `FakeClient` (a `*fakeEmbeddingClient`).
- Existing tests construct `WorkerDeps{}` literally inside each test — there is no harness option function for `BatchSize`, `MaxConsecutiveFailures`, or anything else. Set these as struct-literal fields.
- The fake client today exposes only `FailNext(n int)` which returns the canned string `"simulated embed failure (call %d)"`. **It cannot return `ErrPermanent4xx`.** Step 0 below extends it.
- Pending-row count is asserted with `assertPending(t, db, gen, want)` (a free function), not a method.

The test code in the steps below uses these real names.

- [ ] **Step 0: Extend `fakeEmbeddingClient` with a per-call programmable error**

In `internal/vector/embed/testsupport_test.go`, add an `OnEmbed` callback to the fake client. When non-nil, it overrides the default behavior; otherwise the existing `FailNext`/deterministic-vector path runs.

```go
type fakeEmbeddingClient struct {
    dim       int
    failN     int
    calls     int
    preReturn func()
    LastInputs []string

    // OnEmbed, if non-nil, replaces the default Embed behavior with
    // a caller-provided closure. Used by tests that need to vary
    // returned errors per call (e.g. fail multi-msg batches with
    // ErrPermanent4xx, succeed on singletons).
    OnEmbed func(inputs []string) ([][]float32, error)
}
```

In the `Embed` method, dispatch to `OnEmbed` first when set:

```go
func (c *fakeEmbeddingClient) Embed(_ context.Context, inputs []string) ([][]float32, error) {
    c.calls++
    if c.OnEmbed != nil {
        out, err := c.OnEmbed(inputs)
        if err == nil {
            c.LastInputs = append(c.LastInputs[:0], inputs...)
        }
        return out, err
    }
    // ... existing FailNext + deterministic vector path unchanged ...
}
```

This is a non-test-visible change but it's still a test-only file, so commit it with the rest of Task 5.

- [ ] **Step 1: Write the failing test for the happy path (multi-msg batch, all singletons succeed)**

In `internal/vector/embed/worker_test.go`, add. Mirror the construction style of `TestWorker_EmptyPreprocessedMessagesDrainedFromQueue` (closest cousin) and `TestWorker_DrainsPendingEndToEnd`.

```go
func TestWorker_DownshiftDrain_HappyPath_AllSingletonsSucceed(t *testing.T) {
    f := newWorkerFixture(t, 3)
    f.FakeClient.OnEmbed = func(inputs []string) ([][]float32, error) {
        if len(inputs) > 1 {
            return nil, fmt.Errorf("embed: HTTP 400: too long: %w", ErrPermanent4xx)
        }
        // Singleton: return one deterministic vector.
        v := make([]float32, 4)
        v[0] = 1
        return [][]float32{v}, nil
    }
    w := NewWorker(WorkerDeps{
        Backend:       f.Backend,
        VectorsDB:     f.VectorsDB,
        MainDB:        f.MainDB,
        Client:        f.FakeClient,
        BatchSize:     3,
    })
    res, err := w.RunOnce(context.Background(), f.BuildingGen)
    if err != nil {
        t.Fatalf("RunOnce: %v", err)
    }
    if res.Succeeded != 3 {
        t.Fatalf("Succeeded: got %d, want 3", res.Succeeded)
    }
    if res.Failed != 0 {
        t.Fatalf("Failed: got %d, want 0", res.Failed)
    }
    assertPending(t, f.VectorsDB, int64(f.BuildingGen), 0)
}
```

`ErrPermanent4xx` is an exported sentinel from the `embed` package, but this test file IS the `embed` package (same dir), so reference it bare as `ErrPermanent4xx`.

- [ ] **Step 2: Run the test and confirm it fails**

```bash
go test -tags sqlite_vec ./internal/vector/embed/ -run TestWorker_DownshiftDrain_HappyPath -v
```

Expected: FAIL — likely `Failed: got 3, want 0` because the current code releases the whole batch on error and counts a failure.

- [ ] **Step 3: Implement `downshiftDrain`**

Add this method to `internal/vector/embed/worker.go` (place it after `embedBatch` for locality):

```go
// downshiftDrain handles a non-retryable 4xx on a multi-message batch
// by walking the same already-claimed IDs one at a time. The IDs
// remain owned under the caller's claim_token throughout the drain,
// so we never Release/re-Claim them — that would race other workers.
//
// Returned `embedded` is the count of singletons that successfully
// embedded; the caller uses this to decide whether to reset
// consecutiveFailures (>=1 means real progress was made) or preserve
// the upstream +1 (==0 means every message was dropped, which on a
// fully misconfigured endpoint should still trip the abort cap).
//
// Returned `err` is non-nil only for non-4xx interruptions
// (transient errors that exhausted retries inside embedBatch, upsert
// failures, complete failures, ctx cancellation). The caller treats
// these like the existing upsert-error path: increment
// consecutiveFailures and abort if the cap is reached.
//
// Singleton 4xxs are dropped (Complete with no embedding, mirroring
// the empty-input drain) and do NOT count toward consecutiveFailures
// — the empty-input fix already established this pattern. The
// caller's "drain embedded zero" rule provides the safety net
// against silent total-drop.
func (w *Worker) downshiftDrain(
    ctx context.Context,
    gen vector.GenerationID,
    token string,
    ids []int64,
    res *RunResult,
) (embedded int, err error) {
    for _, id := range ids {
        select {
        case <-ctx.Done():
            return embedded, ctx.Err()
        default:
        }

        batchStart := time.Now()
        eb, e := w.embedBatch(ctx, []int64{id})
        if e != nil {
            // Singleton 4xx: drop. Anything else: bail.
            if errors.Is(e, ErrPermanent4xx) {
                w.deps.Log.Warn("dropping pending message after singleton 4xx",
                    "gen", gen, "id", id, "error", e)
                if cerr := w.q.Complete(ctx, gen, token, []int64{id}); cerr != nil {
                    return embedded, fmt.Errorf("complete drop: %w", cerr)
                }
                res.Failed++
                continue
            }
            return embedded, e
        }
        // Drain orphans (missing or empty after preprocess) the same
        // way the main loop does, but at singleton granularity.
        if len(eb.chunks) == 0 {
            drop := append(append([]int64(nil), eb.missing...), eb.empty...)
            if len(drop) > 0 {
                if cerr := w.q.Complete(ctx, gen, token, drop); cerr != nil {
                    return embedded, fmt.Errorf("complete drop: %w", cerr)
                }
                res.Failed += len(drop)
            }
            continue
        }
        if uerr := w.deps.Backend.Upsert(ctx, gen, eb.chunks); uerr != nil {
            return embedded, fmt.Errorf("upsert: %w", uerr)
        }
        if cerr := w.q.Complete(ctx, gen, token, eb.embeddedIDs); cerr != nil {
            return embedded, fmt.Errorf("complete: %w", cerr)
        }
        res.Truncated += eb.truncated
        embedded += len(eb.embeddedIDs)

        if w.deps.Progress != nil {
            batchChars := 0
            for _, c := range eb.chunks {
                batchChars += c.SourceCharLen
            }
            w.deps.Progress(ProgressReport{
                Done:         res.Succeeded + embedded,
                TotalPending: w.deps.TotalPending,
                BatchMsgs:    len(eb.embeddedIDs),
                BatchChars:   batchChars,
                BatchElapsed: time.Since(batchStart),
                RunElapsed:   time.Since(w.runStart), // see step 4
            })
        }
    }
    return embedded, nil
}
```

You'll also need a way to read `runStart` in the drain. The cleanest: stash it on the worker for the duration of `RunOnce`. In `RunOnce` (where `runStart` is currently a local), promote it to a field:

```go
// In Worker:
type Worker struct {
    deps     WorkerDeps
    q        *Queue
    runStart time.Time // valid only during a RunOnce call
}

// In RunOnce, replace the existing local with:
w.runStart = time.Now()
```

Ensure `errors` is imported in `worker.go`. The current import block has `context, database/sql, fmt, log/slog, strings, time, unicode/utf8` — `errors` is missing and needs to be added for the `errors.Is` check below.

- [ ] **Step 4: Wire `downshiftDrain` into `RunOnce`'s failure branch**

In `RunOnce` find the embedBatch error branch (around lines 211-225 in the current file):

```go
eb, err := w.embedBatch(ctx, ids)
if err != nil {
    res.Failed += len(ids)
    if rerr := w.q.Release(ctx, gen, token, ids); rerr != nil {
        w.deps.Log.Error("release after embed failure", "error", rerr)
    }
    w.deps.Log.Warn("embed batch failed", "gen", gen, "ids", len(ids), "error", err)
    consecutiveFailures++
    lastErr = err
    if consecutiveFailures >= w.deps.MaxConsecutiveFailures {
        return res, fmt.Errorf("embed worker aborting after %d consecutive failures: %w",
            consecutiveFailures, lastErr)
    }
    continue
}
```

Replace with:

```go
eb, err := w.embedBatch(ctx, ids)
if err != nil {
    consecutiveFailures++
    lastErr = err
    w.deps.Log.Warn("embed batch failed", "gen", gen, "ids", len(ids), "error", err)

    if errors.Is(err, ErrPermanent4xx) && len(ids) > 1 {
        // Drain the failing batch one message at a time. The IDs
        // stay claimed under our token; we either embed or drop
        // each one, so res accounting happens inside the drain
        // (don't pre-charge res.Failed here).
        embedded, drainErr := w.downshiftDrain(ctx, gen, token, ids, &res)
        res.Succeeded += embedded
        if drainErr != nil {
            // Non-4xx interruption (transient retries exhausted,
            // upsert/complete failure, ctx cancel). Whatever IDs
            // remained un-Completed by the drain stay claimed for
            // ReclaimStale to recover. Increment cap (drainErr is
            // a fresh failure on top of the upstream +1 we already
            // booked).
            consecutiveFailures++
            lastErr = drainErr
            if consecutiveFailures >= w.deps.MaxConsecutiveFailures {
                return res, fmt.Errorf("embed worker aborting after %d consecutive failures: %w",
                    consecutiveFailures, lastErr)
            }
            continue
        }
        if embedded > 0 {
            consecutiveFailures = 0
        }
        // If embedded == 0 we leave consecutiveFailures at the
        // upstream +1 — fully misconfigured endpoints still trip
        // the cap with the original 4xx body in lastErr.
        continue
    }

    // Non-4xx, or singleton 4xx: original release-and-fail path.
    res.Failed += len(ids)
    if rerr := w.q.Release(ctx, gen, token, ids); rerr != nil {
        w.deps.Log.Error("release after embed failure", "error", rerr)
    }
    if consecutiveFailures >= w.deps.MaxConsecutiveFailures {
        return res, fmt.Errorf("embed worker aborting after %d consecutive failures: %w",
            consecutiveFailures, lastErr)
    }
    continue
}
```

Note the `lastErr` survives the drain branch's `continue` so a subsequent abort still reports the original 4xx body — this is the spec's safety net.

- [ ] **Step 5: Run the happy-path test and confirm it passes**

```bash
go test -tags sqlite_vec ./internal/vector/embed/ -run TestWorker_DownshiftDrain_HappyPath -v
```

Expected: PASS. `Succeeded == 3`, `Failed == 0`.

- [ ] **Step 6: Add the partial-drop test**

Append:

```go
func TestWorker_DownshiftDrain_PartialDrop(t *testing.T) {
    f := newWorkerFixture(t, 3)
    var singletonSeen int
    f.FakeClient.OnEmbed = func(inputs []string) ([][]float32, error) {
        if len(inputs) > 1 {
            return nil, fmt.Errorf("embed: HTTP 400: too long: %w", ErrPermanent4xx)
        }
        singletonSeen++
        // Second singleton fails 4xx; first and third succeed.
        if singletonSeen == 2 {
            return nil, fmt.Errorf("embed: HTTP 400: blocked: %w", ErrPermanent4xx)
        }
        v := make([]float32, 4)
        v[0] = 1
        return [][]float32{v}, nil
    }
    w := NewWorker(WorkerDeps{
        Backend:   f.Backend,
        VectorsDB: f.VectorsDB,
        MainDB:    f.MainDB,
        Client:    f.FakeClient,
        BatchSize: 3,
    })
    res, err := w.RunOnce(context.Background(), f.BuildingGen)
    if err != nil {
        t.Fatalf("RunOnce: %v", err)
    }
    if res.Succeeded != 2 {
        t.Errorf("Succeeded: got %d, want 2", res.Succeeded)
    }
    if res.Failed != 1 {
        t.Errorf("Failed (drop count): got %d, want 1", res.Failed)
    }
    assertPending(t, f.VectorsDB, int64(f.BuildingGen), 0) // dropped row Completed, not Released
}
```

- [ ] **Step 7: Run partial-drop test and confirm PASS**

```bash
go test -tags sqlite_vec ./internal/vector/embed/ -run TestWorker_DownshiftDrain_PartialDrop -v
```

Expected: PASS.

- [ ] **Step 8: Add the all-drop / cap-trips test**

This is the safety-net case. Set `MaxConsecutiveFailures = 2` so the cap trips quickly. Every batch (multi and singleton) returns `ErrPermanent4xx`.

```go
func TestWorker_DownshiftDrain_AllDrop_StillTripsCap(t *testing.T) {
    f := newWorkerFixture(t, 6) // two batches of 3
    f.FakeClient.OnEmbed = func(inputs []string) ([][]float32, error) {
        return nil, fmt.Errorf("embed: HTTP 400: misconfigured: %w", ErrPermanent4xx)
    }
    w := NewWorker(WorkerDeps{
        Backend:                f.Backend,
        VectorsDB:              f.VectorsDB,
        MainDB:                 f.MainDB,
        Client:                 f.FakeClient,
        BatchSize:              3,
        MaxConsecutiveFailures: 2,
    })
    _, err := w.RunOnce(context.Background(), f.BuildingGen)
    if err == nil {
        t.Fatalf("expected abort error, got nil")
    }
    if !strings.Contains(err.Error(), "consecutive failures") {
        t.Errorf("expected abort message, got %v", err)
    }
    if !strings.Contains(err.Error(), "misconfigured") {
        t.Errorf("expected original 4xx body in error, got %v", err)
    }
}
```

- [ ] **Step 9: Run the all-drop test and confirm PASS**

```bash
go test -tags sqlite_vec ./internal/vector/embed/ -run TestWorker_DownshiftDrain_AllDrop -v
```

Expected: PASS. The error message must contain both "consecutive failures" and "misconfigured" (the body text).

- [ ] **Step 10: Add the singleton-4xx-outside-drain test**

A `BatchSize=1` claim that 4xxes hits the existing release-and-fail path with no downshift attempted. The cap default is 5 and the queue holds one message, so the run won't abort — it'll loop, fail, release, and exit when the next claim returns empty (Release on a single id puts it back into pending).

Actually — release puts the row back, and the next iteration claims it again, fails again, releases again, in a tight loop until the cap trips. So with default cap = 5 the run aborts after 5 retries. Make this explicit:

```go
func TestWorker_SingletonBatch_4xx_FollowsExistingPath(t *testing.T) {
    f := newWorkerFixture(t, 1)
    f.FakeClient.OnEmbed = func(inputs []string) ([][]float32, error) {
        return nil, fmt.Errorf("embed: HTTP 400: bad: %w", ErrPermanent4xx)
    }
    w := NewWorker(WorkerDeps{
        Backend:                f.Backend,
        VectorsDB:              f.VectorsDB,
        MainDB:                 f.MainDB,
        Client:                 f.FakeClient,
        BatchSize:              1,
        MaxConsecutiveFailures: 3,
    })
    _, err := w.RunOnce(context.Background(), f.BuildingGen)
    if err == nil {
        t.Fatalf("expected abort after cap, got nil")
    }
    if !strings.Contains(err.Error(), "consecutive failures") {
        t.Errorf("expected cap abort message, got %v", err)
    }
    // Pending row should still be present (Released by the final
    // failing iteration).
    assertPending(t, f.VectorsDB, int64(f.BuildingGen), 1)
}
```

If you find the worker takes a different path here (e.g. an early exit before retrying), update the assertion to match the actual observed behavior — the contract being tested is "singleton 4xx does not enter the downshift drain", and the failure-cap behavior is whatever the existing release-and-fail path already does.

- [ ] **Step 11: Run singleton-outside-drain test and confirm PASS**

```bash
go test -tags sqlite_vec ./internal/vector/embed/ -run TestWorker_SingletonBatch_4xx -v
```

Expected: PASS.

- [ ] **Step 12: Add the ctx-cancel-mid-drain test**

Cancellation during the drain returns `ctx.Err()` from `downshiftDrain`. The caller's `continue` then loops back to the top of `RunOnce`, where the `if err := ctx.Err(); err != nil { return res, fmt.Errorf("RunOnce: %w", err) }` guard at worker.go:193 catches it. The final error wraps `context.Canceled` via `%w`, so `errors.Is(err, context.Canceled)` matches.

```go
func TestWorker_DownshiftDrain_CtxCancelMidDrain(t *testing.T) {
    f := newWorkerFixture(t, 3)
    ctx, cancel := context.WithCancel(context.Background())
    var singletonCalls int
    f.FakeClient.OnEmbed = func(inputs []string) ([][]float32, error) {
        if len(inputs) > 1 {
            return nil, fmt.Errorf("embed: HTTP 400: %w", ErrPermanent4xx)
        }
        singletonCalls++
        if singletonCalls == 2 {
            cancel()
            return nil, context.Canceled
        }
        v := make([]float32, 4)
        v[0] = 1
        return [][]float32{v}, nil
    }
    w := NewWorker(WorkerDeps{
        Backend:   f.Backend,
        VectorsDB: f.VectorsDB,
        MainDB:    f.MainDB,
        Client:    f.FakeClient,
        BatchSize: 3,
    })
    _, err := w.RunOnce(ctx, f.BuildingGen)
    if !errors.Is(err, context.Canceled) {
        t.Fatalf("expected context.Canceled, got %v", err)
    }
    // One singleton was embedded; the remaining IDs stay claimed
    // (status='claimed' in pending_embeddings — ReclaimStale will
    // recover them on the next run).
    assertPending(t, f.VectorsDB, int64(f.BuildingGen), 2)
}
```

`assertPending` counts all pending rows for the gen including claimed-but-not-completed ones; consult its definition to confirm if the assertion above matches reality, and adjust the expected count if needed (the contract being tested is "drain stops cleanly on cancellation and remaining rows are not lost").

- [ ] **Step 13: Run ctx-cancel test and confirm PASS**

```bash
go test -tags sqlite_vec ./internal/vector/embed/ -run TestWorker_DownshiftDrain_CtxCancelMidDrain -v
```

Expected: PASS.

- [ ] **Step 14: Run all worker tests to confirm nothing else regressed**

```bash
go test -tags sqlite_vec ./internal/vector/embed/ -v
```

Expected: all PASS.

- [ ] **Step 15: Commit**

```bash
go fmt ./...
go vet -tags sqlite_vec ./...
git add internal/vector/embed/worker.go internal/vector/embed/worker_test.go
git commit -m "embed worker: drain oversize batches via singleton retry on 4xx"
```

---

## Task 6: End-to-end verification with `make test` and `go vet`

**Files:** None changed; this is a verification gate.

- [ ] **Step 1: Run the full Go test suite**

```bash
make test
```

Expected: all packages PASS.

- [ ] **Step 2: Run `go vet` with the build tag**

```bash
go vet -tags sqlite_vec ./...
```

Expected: no diagnostics.

- [ ] **Step 3: Run the linter**

```bash
make lint
```

Expected: clean.

- [ ] **Step 4: Confirm formatting**

```bash
go fmt ./...
git diff --exit-code
```

Expected: no diff (anything `go fmt` produced should already be in the prior commits).

- [ ] **Step 5: If anything in steps 1-4 fails, fix and re-run before declaring done.**

No commit on this step unless step 4 produced a fixup; if so, commit as `chore: gofmt fixups`.

---

## Task 7: Update CLAUDE.md mention (optional)

**Files:**
- Modify: `CLAUDE.md` (only if the existing `[vector.embeddings]` example references would mislead readers without the new key)

- [ ] **Step 1: Inspect**

Search `CLAUDE.md` for `[vector.embeddings]` and `batch_size`. If neither appears in a configuration example, skip this task.

```bash
grep -n "vector.embeddings\|batch_size" CLAUDE.md
```

- [ ] **Step 2: Add `eta_window` to the example if one exists, otherwise skip.**

If skipping, do not commit.

- [ ] **Step 3: Commit (only if there was a real edit)**

```bash
git add CLAUDE.md
git commit -m "docs: note vector.embeddings.eta_window in CLAUDE.md"
```

---

## Notes for the Implementer

- **Do not refactor for fun.** The two changes are scoped to specific files in the spec; resist any urge to clean up adjacent code that isn't part of either change.
- **The harness names in Task 5's tests are illustrative.** Read `internal/vector/embed/testsupport_test.go` and `worker_test.go` carefully; match whatever conventions exist (e.g. table-driven assertions, mock client setters, harness option functions). Existing tests like `TestWorker_EmptyPreprocessedMessagesDrainedFromQueue` are the closest cousins of the new ones.
- **The `runStart` field promotion in Task 5 is the only worker-shape change.** Avoid adding other state to the `Worker` struct; the rest of the drain operates through method parameters.
- **Commit boundaries matter for review.** The plan groups commits as: (1) config field, (2) ring buffer + tests, (3) printer wiring, (4) sentinel + client tests, (5) downshift drain + tests, (6) verification, (7) optional doc. Keep them in that order.
- **If `make test` fails for unrelated reasons** (flaky test, pre-existing breakage), surface that to the user rather than papering over it.
