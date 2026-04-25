package bench

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/wesm/msgvault/internal/vector/embed"
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
	if f.embedDur > 0 {
		time.Sleep(f.embedDur)
	}
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

func TestRunEndpoint_EmptyMessages(t *testing.T) {
	fake := &fakeEmbedClient{dim: 8}
	res, err := RunEndpoint(context.Background(), EndpointInputs{
		Messages:  nil,
		Client:    fake,
		BatchSize: 32,
		Workers:   1,
	})
	if err != nil {
		t.Fatalf("RunEndpoint: %v", err)
	}
	if res.Result().Msgs != 0 {
		t.Errorf("expected 0 msgs")
	}
	if fake.callCount != 0 {
		t.Errorf("expected 0 client calls; got %d", fake.callCount)
	}
}

func TestRunEndpoint_RejectsBadConfig(t *testing.T) {
	fake := &fakeEmbedClient{dim: 8}
	_, err := RunEndpoint(context.Background(), EndpointInputs{
		Messages:  []PreparedMessage{{ID: 1, Text: "x", Chars: 1}},
		Client:    fake,
		BatchSize: 0,
		Workers:   1,
	})
	if err == nil {
		t.Fatal("expected error for BatchSize=0")
	}
	_, err = RunEndpoint(context.Background(), EndpointInputs{
		Messages:  []PreparedMessage{{ID: 1, Text: "x", Chars: 1}},
		Client:    fake,
		BatchSize: 1,
		Workers:   0,
	})
	if err == nil {
		t.Fatal("expected error for Workers=0")
	}
}

// stubEmbedClient is a configurable fake. Each call records its
// inputs and either succeeds (returning len(inputs) zero vectors of
// the configured dim) or returns the next-queued error. Safe for
// concurrent use.
type stubEmbedClient struct {
	mu        sync.Mutex
	dim       int
	callDelay time.Duration
	nextErrs  []error // popped FIFO
	callCount int
}

func (s *stubEmbedClient) Embed(ctx context.Context, inputs []string) ([][]float32, error) {
	s.mu.Lock()
	s.callCount++
	var err error
	if len(s.nextErrs) > 0 {
		err = s.nextErrs[0]
		s.nextErrs = s.nextErrs[1:]
	}
	delay := s.callDelay
	s.mu.Unlock()
	if delay > 0 {
		time.Sleep(delay)
	}
	if err != nil {
		return nil, err
	}
	out := make([][]float32, len(inputs))
	for i := range out {
		out[i] = make([]float32, s.dim)
	}
	return out, nil
}

func TestRunEndpoint_WarmupExcluded(t *testing.T) {
	// 3 messages × batch 1 = 3 batches per worker. With Workers=1
	// and WarmupBatches=1, the first batch is warmup (excluded from
	// throughput aggregates). Aggregate should reflect only the
	// remaining 2 batches.
	msgs := []PreparedMessage{
		{ID: 1, Text: "a", Chars: 1},
		{ID: 2, Text: "b", Chars: 1},
		{ID: 3, Text: "c", Chars: 1},
	}
	fake := &stubEmbedClient{dim: 8}
	res, err := RunEndpoint(context.Background(), EndpointInputs{
		Messages: msgs, Client: fake, BatchSize: 1, Workers: 1, WarmupBatches: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	agg := res.Result()
	if agg.Msgs != 2 {
		t.Errorf("Msgs after warmup excl: got %d, want 2", agg.Msgs)
	}
	if fake.callCount != 3 {
		t.Errorf("callCount: got %d, want 3 (all batches still embedded, warmup just excluded from agg)", fake.callCount)
	}
}

func TestRunEndpoint_MultiWorker_Concurrent(t *testing.T) {
	// 16 messages × batch 4 = 4 batches. With 4 workers and a slow
	// fake (50ms per call), wall time should be roughly 50ms (one
	// batch per worker, parallel) — much less than 200ms (sequential).
	// Use a generous threshold to avoid flakiness.
	msgs := make([]PreparedMessage, 16)
	for i := range msgs {
		msgs[i] = PreparedMessage{ID: int64(i + 1), Text: "x", Chars: 1}
	}
	fake := &stubEmbedClient{dim: 8, callDelay: 50 * time.Millisecond}
	start := time.Now()
	res, err := RunEndpoint(context.Background(), EndpointInputs{
		Messages: msgs, Client: fake, BatchSize: 4, Workers: 4, WarmupBatches: 0,
	})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	if res.Result().Msgs != 16 {
		t.Errorf("Msgs: got %d, want 16", res.Result().Msgs)
	}
	if elapsed > 150*time.Millisecond {
		t.Errorf("multi-worker should run concurrently; elapsed=%v want <150ms", elapsed)
	}
}

func TestRunEndpoint_ErrorClassification(t *testing.T) {
	// Use a real embed.ErrPermanent4xx, a 500-string error, a 429-
	// string error, and a *net.OpError. Verify each lands in the
	// correct counter.
	// Use error shapes that match what embed.Client actually produces
	// (see internal/vector/embed/client.go) — no trailing words, just
	// the bare status code. The previous-pass test masked a real bug
	// in the classifier by appending words after the digits.
	err4xx := embed.ErrPermanent4xx
	err5xx := errors.New("embed: giving up after 3 attempts: embed: HTTP 500")
	err429 := errors.New("embed: HTTP 429 (rate limited)")
	errNet := &net.OpError{Op: "dial", Err: errors.New("connection refused")}

	fake := &stubEmbedClient{
		dim:      8,
		nextErrs: []error{err4xx, err5xx, err429, errNet},
	}
	msgs := make([]PreparedMessage, 4)
	for i := range msgs {
		msgs[i] = PreparedMessage{ID: int64(i + 1), Text: "x", Chars: 1}
	}
	res, err := RunEndpoint(context.Background(), EndpointInputs{
		Messages: msgs, Client: fake, BatchSize: 1, Workers: 1,
	})
	if err != nil {
		t.Fatalf("RunEndpoint should not abort on per-batch errors; got %v", err)
	}
	agg := res.Result()
	if agg.Errors4xx != 1 {
		t.Errorf("Errors4xx: got %d, want 1", agg.Errors4xx)
	}
	if agg.Errors5xx != 1 {
		t.Errorf("Errors5xx: got %d, want 1", agg.Errors5xx)
	}
	if agg.Errors429 != 1 {
		t.Errorf("Errors429: got %d, want 1", agg.Errors429)
	}
	if agg.ErrorsNet != 1 {
		t.Errorf("ErrorsNet: got %d, want 1", agg.ErrorsNet)
	}
	if agg.Msgs != 0 {
		t.Errorf("Msgs (all errored): got %d, want 0", agg.Msgs)
	}
}
