package bench

import (
	"math"
	"slices"
	"sync"
	"time"
)

// Aggregator accumulates per-batch metrics during a run and produces
// a final summary. Only non-warmup, non-error batches contribute to
// throughput aggregates; error counters accumulate separately so the
// run's wall time still includes all activity.
//
// Aggregator is safe for concurrent AddBatch / AddError / AddRetries
// calls from multiple worker goroutines. Start / Stop / Result are
// expected to be called only from the orchestrating goroutine, before
// or after worker goroutines run.
type Aggregator struct {
	mu           sync.Mutex
	batchElapsed []time.Duration
	msgs         int
	chars        int
	embedTotal   time.Duration
	truncated    int

	errors4xx, errors5xx, errors429, errorsNet int
	retries                                    int

	claimMs, upsertMs, completeMs time.Duration

	runStart   time.Time
	runStarted bool
	runEnd     time.Time
}

// NewAggregator returns a fresh aggregator. Call Start before
// recording batches and Stop when the run ends.
func NewAggregator() *Aggregator { return &Aggregator{} }

// Start records the run's start timestamp.
func (a *Aggregator) Start() {
	a.runStart = time.Now()
	a.runStarted = true
}

// Stop records the run's end timestamp.
func (a *Aggregator) Stop() { a.runEnd = time.Now() }

// AddBatch records a successful, non-warmup batch. Safe for
// concurrent use from multiple worker goroutines.
func (a *Aggregator) AddBatch(msgs, chars int, elapsed, embed, claim, upsert, complete time.Duration, truncated int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.batchElapsed = append(a.batchElapsed, elapsed)
	a.msgs += msgs
	a.chars += chars
	a.embedTotal += embed
	a.truncated += truncated
	a.claimMs += claim
	a.upsertMs += upsert
	a.completeMs += complete
}

// AddError increments the counter for the given error class. Unknown
// classes are silently ignored — callers should pass "4xx", "5xx",
// "429", or "network". Safe for concurrent use.
func (a *Aggregator) AddError(class string) {
	a.mu.Lock()
	defer a.mu.Unlock()
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

// AddRetries adds n to the run-level retry counter. Safe for
// concurrent use.
func (a *Aggregator) AddRetries(n int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.retries += n
}

// Aggregate is the final summary.
type Aggregate struct {
	Msgs       int
	Chars      int
	Truncated  int
	ElapsedMs  int64 // wall time of the run; may include warmup
	EmbedMsSum int64 // sum of embed phase durations across non-warmup batches

	MsgPerSec float64
	MsPerMsg  float64
	UsPerChar float64

	BatchP50, BatchP95, BatchP99, BatchMax float64

	Errors4xx, Errors5xx, Errors429, ErrorsNet int
	Retries                                    int

	ClaimMs, UpsertMs, CompleteMs int64
}

// Result computes the aggregate. Safe to call before AddBatch (rates
// will be zero, percentiles will be NaN). Holds the same lock as the
// Add* methods so a Result snapshot taken concurrently with worker
// activity is internally consistent.
func (a *Aggregator) Result() Aggregate {
	a.mu.Lock()
	defer a.mu.Unlock()
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

// PercentileMs returns the requested percentile (0..100) of ds in
// milliseconds, with linear interpolation between adjacent ranks.
// Returns NaN when ds is empty. p<=0 returns the minimum; p>=100
// returns the maximum.
func PercentileMs(ds []time.Duration, p float64) float64 {
	if len(ds) == 0 {
		return math.NaN()
	}
	sorted := append([]time.Duration(nil), ds...)
	slices.Sort(sorted)
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

// MaxMs returns the maximum of ds in milliseconds, or NaN on empty.
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
