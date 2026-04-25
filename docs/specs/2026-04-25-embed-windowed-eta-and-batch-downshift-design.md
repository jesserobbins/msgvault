# Embed worker: windowed ETA + oversize-batch downshift

Date: 2026-04-25
Status: Implemented (`jesse/embed-empty-input`, commits caef65b…010ce61). Verified end-to-end against a local OpenAI-compatible endpoint that returns HTTP 413 on payload-too-large batches; downshift fires correctly and the windowed rate stays meaningful through the drain.
Scope: `internal/vector/embed/`, `internal/vector/config.go`, `cmd/msgvault/cmd/embed_vector.go`. Two roborev refinement passes added small consistency edges in `internal/scheduler/embed_job.go` and `internal/vector/embed/client.go` (see "Refinements during implementation" below).

## Motivation

Two issues affect long `msgvault build-embeddings` runs against local OpenAI-compatible endpoints:

1. The progress line's ETA is computed from `Done / RunElapsed` — a cumulative average over the entire run. After a few minutes the rate barely moves, so the ETA stays wrong long after the actual throughput has shifted (model warm-up, thermal throttling, server load changes). Operators backfilling 20+ years of email want a recent-throughput estimate, not a lifetime one.

2. When a single batch exceeds the embedding endpoint's input limit (model context, request-body cap, server-specific token ceiling), the worker fails the whole batch, releases all IDs, retries the same batch, fails again, and trips the consecutive-failure abort cap (5 by default). The user has to lower `batch_size` globally and restart, even though typically only one fat message is the cause.

This spec covers a windowed-rate ETA and a localized batch-size downshift on non-retryable 4xx, both scoped to the embed worker.

## Scope

In scope:

- `embed.Worker` and its `Progress` callback.
- `embed.Client` error surface (a sentinel for non-retryable 4xx).
- `EmbeddingsConfig` (one new TOML key).
- The CLI progress printer in `cmd/msgvault/cmd/embed_vector.go`.

Out of scope:

- Token-aware batch packing (compute token budget from message size before claiming). Future work — useful but a larger change.
- Per-message permanent-failure quarantine table. Drop-and-log mirrors the existing empty-input behavior; a quarantine column can be added later if visibility becomes a need.
- CLI flag override for the window size. TOML-only for the first cut; not needed until somebody actually wants per-run tuning.

## Part A — Windowed ETA

### Behavior

Replace cumulative `msgs/sec` in the progress line with a sliding-window rate computed over the last N successful batches, where rate = `sum(BatchMsgs) / sum(BatchElapsed)` over the window. N defaults to 10 and is configured via `[vector.embeddings] eta_window`.

Why message-weighted rather than per-batch-mean: the downshift behavior in Part B will produce streaks of `BatchSize=1` batches when the worker is draining a poison message. A simple mean of per-batch rates would treat a 1-msg batch and a 100-msg batch equally and tank the displayed throughput. `sum(msgs)/sum(elapsed)` makes small batches contribute proportionally less and keeps the ETA stable through downshift drains.

Why "last N batches" rather than "last N seconds" or "last N messages": the user-facing knob is most natural as a batch count, the data points arrive on batch boundaries anyway, and a ring buffer of 10 entries is trivial to maintain. Time-windowed averages would require timestamp scanning per progress event.

### Component design

The smoothing lives **in the printer**, not in `embed.Worker`. The worker already emits a `ProgressReport` per successful batch with `BatchMsgs` and `BatchElapsed`; that's everything the windowed rate needs. Keeping the smoothing out of the worker:

- Leaves `ProgressReport` a pure event payload (no derived/cached fields).
- Lets future consumers (a TUI, structured logs, metrics) pick a different smoothing strategy without changing the worker API.
- Makes the windowed rate testable as a small standalone helper.

New helper in `cmd/msgvault/cmd/embed_vector.go` (or a sibling file in the same package):

```go
type rateWindow struct {
    cap         int
    msgs        []int           // ring buffer
    elapsed     []time.Duration // ring buffer
    head, count int
}

func (w *rateWindow) Add(msgs int, d time.Duration) { /* … */ }
func (w *rateWindow) Rate() float64                 { /* sum(msgs)/sum(elapsed) seconds */ }
```

`newProgressPrinter` takes a window size (passed in from config) and owns one `rateWindow`. Each progress event:

1. `window.Add(p.BatchMsgs, p.BatchElapsed)` — **fires before the 2-second throttle return**, so every event contributes to the window even when the line itself is suppressed by the throttle. Without this ordering a fast downshift drain would leak almost all of its samples and the `(last K)` annotation would never reflect drain throughput.
2. If the throttle suppresses the line, return without printing.
3. Otherwise compute `windowedRate := window.Rate()` and `samples := window.Samples()`.
4. Compute ETA as `remaining / windowedRate` instead of from cumulative `Done / RunElapsed`.

Cumulative `Done / RunElapsed` is dropped from the displayed line — the windowed rate replaces it. We do not display two rates; the line is already dense.

### Output format

Existing line:

```
progress: 1234/50000 (2.5%) — 412 msg/s, 2.4 ms/msg, 1.83 µs/char, ETA 1h54m12s
```

New line (same shape; the rate is now windowed):

```
progress: 1234/50000 (2.5%) — 412 msg/s (last 10), 2.4 ms/msg, 1.83 µs/char, ETA 1h54m12s
```

The `(last 10)` annotation makes the smoothing explicit. When the window has not yet filled (first N-1 batches of a run), still display the rate computed from the partial window — same formula, just fewer samples — and annotate `(last K)` with the actual sample count.

### Edge cases

- **Window empty** (first event before `Add`, or zero-msg batches only): `Rate()` returns 0; the printer falls back to the no-ETA branch (`progress: %d embedded — …`) exactly as today.
- **Zero `BatchElapsed`** on an event: skipped (don't add) to avoid contaminating the window with a divide-by-zero contributor; the next event re-establishes a healthy sample.
- **Window size of 0 or negative** in TOML: defaulted to 10 in `applyDefaults`.

## Part B — Oversize-batch downshift

### Trigger

When `embedBatch` returns a non-retryable 4xx error AND `len(ids) > 1`, the worker enters a downshift drain for that batch's IDs instead of releasing them and incrementing the failure cap. The downshift is scoped to those IDs — once they are drained, the next claim returns to the configured `BatchSize`.

A 4xx on `len(ids) == 1` skips the downshift path (it cannot help) and follows the existing failure-cap path. The same `ErrPermanent4xx` sentinel is still returned from the client and propagated up through `embedBatch` (which wraps with `fmt.Errorf("embed: %w", err)` — the `%w` must be preserved so `errors.Is` keeps working at every layer); singleton batches simply don't trigger the split.

### How "non-retryable 4xx" is identified

`embed.Client` already distinguishes transient (5xx, 429, network, decode) errors via the internal `*retryError` and surfaces them through the retry loop; everything else (4xx other than 429) is returned as a plain error from `Embed`. We add a sentinel:

```go
// In internal/vector/embed/client.go
var ErrPermanent4xx = errors.New("embed: non-retryable 4xx response")
```

The 4xx branch in `doOnce` wraps its returned error with `%w` of `ErrPermanent4xx` so the worker can detect it via `errors.Is`. The HTTP status and (already-included) bounded body remain in the error message.

The choice not to string-match phrases like "input too long" is deliberate: phrases vary across servers, and the empty-input fix already drains messages that legitimately can't embed. Treating any non-retryable 4xx on a multi-message batch as "split and try one at a time" is the smallest general rule.

### Downshift drain

Inside the worker's batch loop, after `embedBatch` fails (and after the upstream `consecutiveFailures++` and `lastErr = err` assignments):

```go
if errors.Is(err, embed.ErrPermanent4xx) && len(ids) > 1 {
    log.Info("embed: downshifting to BatchSize=1 to drain oversize batch",
        "gen", gen, "batch_size", len(ids))
    embedded, dropped, drainErr := w.downshiftDrain(ctx, gen, token, ids, &res)
    if drainErr != nil {
        log.Info("embed: downshift drain interrupted",
            "gen", gen, "batch_size", len(ids),
            "embedded", embedded, "dropped", dropped,
            "remaining_claimed", len(ids)-embedded-dropped, "error", drainErr)
    } else {
        log.Info("embed: downshift drain complete; resuming configured batch size",
            "gen", gen, "batch_size", len(ids),
            "embedded", embedded, "dropped", dropped)
    }
    res.Succeeded += embedded

    // Reset the cap whenever the drain made forward progress, even
    // when drainErr != nil (a partial drain that succeeded on most
    // singletons but hit a transient error on one is still progress).
    if embedded > 0 {
        consecutiveFailures = 0
    }
    if drainErr != nil {
        consecutiveFailures++
        lastErr = drainErr
        if consecutiveFailures >= w.deps.MaxConsecutiveFailures {
            return res, fmt.Errorf(/* … */)
        }
        continue
    }
    // If embedded == 0 (every singleton dropped 4xx) we leave
    // consecutiveFailures at the upstream +1 — fully misconfigured
    // endpoints still trip the cap with the original 4xx body in
    // lastErr. The cap check is INLINE (not deferred to the next
    // iteration's failure) because an all-drop drain may have
    // emptied the queue, and the next Claim returning zero rows
    // would otherwise let RunOnce exit cleanly without
    // re-evaluating the cap.
    if consecutiveFailures >= w.deps.MaxConsecutiveFailures {
        return res, fmt.Errorf(/* … */)
    }
    continue
}
```

Signature: `func (w *Worker) downshiftDrain(ctx, gen, token, ids []int64, res *RunResult) (embedded int, dropped int, err error)`. The `embedded` count drives the cap-reset rule; `dropped` is purely for the exit-log summary (sum of singleton 4xx drops + orphan/empty-preprocess drops within the drain); `err` is non-nil only for non-4xx errors that interrupt the drain.

`downshiftDrain` walks the IDs one at a time:

- For each id, call `embedBatch(ctx, []int64{id})`.
  - On success → `Backend.Upsert` + `q.Complete` for that single id; emit a `ProgressReport` with `BatchMsgs=1` so the rateWindow sees the singleton's actual elapsed; increment `embedded`.
  - On `ErrPermanent4xx` failure (the singleton itself is unembeddable) → log a warning with the truncated body, `q.Complete` the id (drop, same drain pattern as the empty-input case), increment `dropped`. **Successful Complete drops do NOT increment `res.Failed`** — dropping an unembeddable message is the worker acknowledging the queue, not a failure to process it. This matches the main loop's behavior for missing/empty-preprocess drops, where `res.Failed` is only charged when `Complete` itself errors. Singleton drops do not increment `consecutiveFailures` (the empty-input fix established this pattern; the caller's "drain embedded zero" rule provides the safety net).
  - On any other error (transient that already exhausted retries, upsert failure, complete failure, ctx cancellation) → return immediately with that error; remaining IDs in the drain stay claimed and will be picked up by `ReclaimStale` on the next run.

If `embedded == 0` — every single message was dropped as a 4xx — `consecutiveFailures` is **not** reset. This is the safety net against silent total-drop on a misconfigured endpoint: the original batch's `consecutiveFailures++` stays on the books, and after `MaxConsecutiveFailures` such all-drop drains the worker aborts with the original 4xx error in `lastErr`, body and all.

### Why split inline rather than re-claim with smaller batch

The IDs are already claimed under the current `claim_token`. Releasing and re-claiming with `BatchSize=1` would race other workers and lose the per-batch isolation. Splitting inline keeps the exact same IDs, owned by the same token, walked one at a time.

### Progress reporting during drain

Each successful singleton embed fires a `ProgressReport` (`BatchMsgs=1`, `BatchElapsed`= the singleton's measured time). This is correct: the rateWindow sees real per-message timings, and because we agreed on `sum(msgs)/sum(elapsed)` weighting, those slow singletons influence the windowed rate proportionally to their tiny size — they don't overwhelm it. The printer's `window.Add` runs before the 2-second throttle return, so every singleton lands in the window even when the printed line is suppressed.

Drops do **not** fire `ProgressReport`. They are logged via `Log.Warn("dropping pending message after singleton 4xx", …)` with the truncated 4xx body.

### Operator visibility

The drain's start and end are also logged at Info level so an operator watching stderr can see when the worker enters and leaves drain mode without inferring it from the warning + rate dip:

- Entry: `"embed: downshifting to BatchSize=1 to drain oversize batch"` with `gen` and `batch_size`.
- Exit (clean): `"embed: downshift drain complete; resuming configured batch size"` with `embedded` and `dropped` counts.
- Exit (interrupted): `"embed: downshift drain interrupted"` with `embedded`, `dropped`, `remaining_claimed`, and the underlying error.

## Config changes

`internal/vector/config.go`:

```go
type EmbeddingsConfig struct {
    Endpoint      string        `toml:"endpoint"`
    // …existing fields…
    BatchSize     int           `toml:"batch_size"`
    Timeout       time.Duration `toml:"timeout"`
    MaxRetries    int           `toml:"max_retries"`
    MaxInputChars int           `toml:"max_input_chars"`
    ETAWindow     int           `toml:"eta_window"` // NEW: progress smoothing window in batches; default 10
}
```

`applyDefaults` defaults `ETAWindow` to 10 if `<= 0`. No validation is needed beyond that — any positive integer is acceptable; the user can choose 1 (no smoothing, equivalent to the per-batch instantaneous rate) or 100 (very smooth, slow to react).

`runEmbed` in `embed_vector.go` passes `cfg.Vector.Embeddings.ETAWindow` into `newProgressPrinter`.

## Error handling and abort semantics

Summary of how `consecutiveFailures` is updated under the new rules:

| Situation                                                                | Δ on counter                                  |
|--------------------------------------------------------------------------|-----------------------------------------------|
| Normal batch succeeds (existing)                                         | reset to 0                                    |
| Main-loop batch embeds ≥ 1 row but the orphan-drain `Complete` fails     | reset to 0 (real progress; orphan failure surfaced via `orphanDrainErr`) |
| Multi-msg batch returns `ErrPermanent4xx`                                | +1 (upstream); downshift drain follows        |
| Drain embeds ≥ 1 message AND drainErr == nil                             | reset to 0                                    |
| Drain embeds ≥ 1 message AND drainErr != nil                             | reset to 0 then +1 from drain error (net = 1) |
| Drain embeds 0 messages AND drainErr == nil (all-drop)                   | unchanged from upstream +1                    |
| Drain embeds 0 messages AND drainErr != nil                              | upstream +1, then +1 from drain error (net = +2) |
| Singleton batch returns `ErrPermanent4xx` outside drain                  | +1 (existing release-and-fail path)           |
| Other existing failure modes                                             | unchanged                                     |

`res.Failed` accounting is symmetric across drain and main loop: a row that is `Complete`d successfully (whether embedded, missing, empty-preprocess, or singleton-4xx-dropped) does **not** count toward `res.Failed`. Only `Complete` errors and embed/upsert errors populate it.

This preserves three important guarantees:

1. **Forward progress is honored.** Any batch — drain or main loop — that actually embeds at least one message resets the cap. A run that's mostly succeeding with the occasional poison message will not abort.
2. **Total breakage still trips the cap.** If every batch downshifts and drains 100% by drop (e.g. wrong model, wrong API shape, server rejecting all inputs), `MaxConsecutiveFailures` upstream-failures still accumulate until abort — and the abort message carries the actual 4xx body, the same fix the empty-input PR established.
3. **All-drop cap check is inline, not deferred.** When a drain embeds zero messages, the worker re-checks the cap immediately rather than waiting for the next iteration's failure to do so. Without this, an all-drop drain that empties the queue would let `RunOnce` exit cleanly via the empty-claim path before the cap was re-evaluated.

## Testing

### Windowed ETA

`cmd/msgvault/cmd/embed_vector_test.go` (or a sibling `_test.go`):

- `rateWindow` table-driven tests:
  - Empty window → `Rate() == 0`.
  - Partial fill (e.g. 3 of 10) → rate computed from 3 samples.
  - Full fill, then one more → oldest sample evicted; new rate reflects only the recent 10.
  - Mixed batch sizes (1, 100, 1) → rate equals `sum(msgs)/sum(elapsed)`, **not** mean of per-batch rates.
  - Zero `elapsed` sample → skipped (rate computed from remaining).
- Printer integration: feed a synthetic sequence of `ProgressReport`s and assert the formatted line shows the windowed rate and `(last K)` annotation.

### Downshift drain

`internal/vector/embed/worker_test.go` already has end-to-end harness machinery; extend with:

- Multi-msg batch where the embed client returns `ErrPermanent4xx` once on the full batch and succeeds on every singleton retry → all rows complete, `res.Succeeded == len(ids)`, `consecutiveFailures` resets.
- Multi-msg batch where the embed client returns `ErrPermanent4xx` on full and on one specific singleton → the bad id is dropped (Complete with no embedding), the rest succeed, no new failure-cap increment from the drop.
- Multi-msg batch where every singleton also fails 4xx → all rows dropped, `consecutiveFailures` unchanged (still at the +1 from the upstream batch failure), next batch's failure increments to 2 — verify the cap eventually trips with `lastErr` being the 4xx body.
- Singleton batch (`BatchSize=1` already) returning `ErrPermanent4xx` → existing path; no downshift attempted; counter +1; verify no infinite recursion.
- Drain interrupted by `ctx.Cancel()` mid-walk → remaining IDs stay claimed; `ReclaimStale` recovers them on next run.

### Client sentinel

`internal/vector/embed/client_test.go`:

- 400/422 response → `errors.Is(err, ErrPermanent4xx) == true`, error body present in `Error()`.
- 500 / 429 / network failure → `errors.Is(err, ErrPermanent4xx) == false` (still flows through `*retryError`).
- 401/403 (auth/permission, also 4xx) → satisfies `ErrPermanent4xx`. Worker will downshift once and then drop everything; the resulting all-drop drain will leave `consecutiveFailures` at +1 each cycle and abort with the 401/403 body — acceptable.

## Documentation

Update `CLAUDE.md` only if a quick-commands example references the new key; otherwise the example config in `docs/embedding_empty_input_issue_draft.md` is the canonical config sample. No new docs file. (Verified at implementation time: `CLAUDE.md` does not show a `[vector.embeddings]` example, so no edit was needed.)

## Refinements during implementation

Two roborev-refine passes surfaced consistency edges the initial design missed. Each became a small follow-up commit; they are listed here so the spec reflects shipped reality.

**Iteration 1 (`c69bf28`):**

- **Symmetric drop accounting.** As noted above, drain `Complete` drops no longer increment `res.Failed`; this matches the main loop's existing treatment of missing/empty drops. The user-visible `Claimed/Succeeded/Failed/Truncated` line is now consistent regardless of which path the worker took.
- **Stale comment in `RunOnce`'s `len(eb.chunks) == 0` branch.** The original comment said "all ids in this batch were missing from main DB"; the recent empty-preprocess drop makes that branch fire on empty-after-preprocess too. Reworded.
- **`SetEmbedJob` doc accuracy in `internal/scheduler/scheduler.go`.** The doc claimed "all-or-nothing" rollback semantics that the implementation only delivers for `ValidateCronExpr`-rejected schedules. Softened to describe what the code actually does (an internal `AddFunc` failure clears the embed job rather than restoring the prior one).
- **Drain partial-success cap reset.** The original design reset `consecutiveFailures` only on `drainErr == nil && embedded > 0`. With `drainErr != nil && embedded > 0`, the counter would stay at upstream+1+drain+1=2 even though substantial real progress had been made. Moved the reset to fire whenever `embedded > 0`, then take a fresh +1 from `drainErr` if any — so a 31-of-32-singletons-succeeded-then-transient-error case lands at 1, not 2.

**Iteration 2 (`f6ee8c8`):**

- **Main-loop orphan-drain symmetry.** Same shape as the drain refinement above: when the embedded rows succeed but the subsequent orphan-drop `Complete` errors, reset `consecutiveFailures` to 0 because real forward progress was made. The orphan-drop failure still surfaces via `orphanDrainErr` on the empty-claim exit, so the cap doesn't need to escalate it.
- **`embed.Client` backoff clamp.** `time.Duration(1<<attempt) * 100ms` had no upper bound; a misconfigured `MaxRetries=30+` would produce hours-long backoffs and `attempt >= 63` would trip undefined shift behavior on `int`. Clamped the shift at 8 (`min(attempt, 8)`) for a max of 25.6 s per attempt. The default `MaxRetries=3` made this benign in practice; the clamp removes the foot-gun.
- **`scheduler.EmbedJob.pickTarget` fingerprint guard.** When `j.Fingerprint == ""` and a building generation exists, the daemon previously fell back to "any building generation" and would auto-activate it once drained — silently swapping the production index to a different model. The CLI's `pickEmbedGeneration` already enforces a fingerprint match for exactly this reason; brought the daemon path into line by refusing to drain when `Fingerprint == ""`.

**Operator visibility (`010ce61`):**

After watching the implementation drain a 200-message HTTP-413 batch in production, the start/end of the drain were not visibly distinguishable from a normal slow stretch in the progress line. Added Info-level entry/exit logs (see "Operator visibility" above) so the drain's lifetime is explicit in stderr. `downshiftDrain`'s signature picked up a `dropped int` return for the exit log; tests assert on `RunResult`, so the signature change was internal-only.

## Open questions

None blocking. Three deferred decisions:

- Token-aware packing (skip downshift entirely by sizing batches correctly up front). Future work.
- Per-message quarantine state for repeat-fail messages. Useful if drop-and-log proves too quiet in practice; revisit if users hit it.
- More intelligent backoff approaches in the downshift drain. The current design walks the failing batch one message at a time on a 4xx — fine when the issue is one fat message in a healthy run, suboptimal when the boundary is somewhere in between (e.g. batch of 50 fails because two messages put it over the limit, but batches of 25 would succeed). Possibilities to revisit if this proves too slow in practice: binary-split the batch (try halves, then quarters), step down to a configurable minimum like `BatchSize/4` before going to 1, or learn a per-run effective max from observed successes. All add complexity; we ship the simple split-to-1 first and graduate based on real workloads.
