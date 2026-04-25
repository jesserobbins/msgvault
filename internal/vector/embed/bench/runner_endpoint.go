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

// RunEndpoint runs N workers consuming preprocessed messages from a
// shared channel and emitting batches against the EmbedClient. The
// returned vectors are discarded — this is a benchmark, not a real
// embed run. Returns when all messages are consumed (success or
// error) or ctx is cancelled.
//
// The first WarmupBatches batches per worker are still embedded but
// are not recorded into the aggregator — they let local servers warm
// up without skewing throughput. Errors from the embed client are
// classified and accumulated in the aggregator's error counters; the
// run does not abort on per-batch failures.
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

	if len(in.Messages) == 0 {
		return agg, nil
	}

	// Channel buffer is generous so feeding does not stall workers.
	ch := make(chan PreparedMessage, in.BatchSize*in.Workers*2)
	go func() {
		defer close(ch)
		for _, m := range in.Messages {
			select {
			case <-ctx.Done():
				return
			case ch <- m:
			}
		}
	}()

	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		runErr error
	)
	setErr := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		if runErr == nil {
			runErr = err
		}
	}

	for range in.Workers {
		wg.Go(func() {
			warmupLeft := in.WarmupBatches
			var batch []PreparedMessage
			for {
				select {
				case <-ctx.Done():
					setErr(ctx.Err())
					return
				case m, ok := <-ch:
					if !ok {
						if len(batch) > 0 {
							sendBatch(ctx, batch, in.Client, agg, &warmupLeft)
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
		})
	}
	wg.Wait()
	if runErr != nil {
		return agg, runErr
	}
	// If the context was canceled but no worker observed it (e.g. the
	// run was small enough that the feeder + workers raced ctx.Done()
	// in their select branches), surface the cancellation so callers
	// can rely on cancel → context.Canceled.
	if err := ctx.Err(); err != nil {
		return agg, err
	}
	return agg, nil
}

// sendBatch is shared between RunEndpoint workers; it embeds the
// batch (timing it), updates aggregator counters, and decrements
// warmupLeft. Errors are classified into the aggregator's error
// counters and the worker continues — RunEndpoint does NOT abort on
// individual batch failures (consistent with the benchmark contract).
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
		if cls := classifyEmbedErr(err); cls != "" {
			agg.AddError(cls)
		}
		return
	}
	if isWarmup {
		return
	}
	// Endpoint mode: embed time and total elapsed are the same; queue
	// and upsert phase fields are not measured here (they're zero).
	agg.AddBatch(len(batch), chars, elapsed, elapsed, 0, 0, 0, truncated)
}
