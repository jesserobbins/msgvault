//go:build sqlite_vec

package bench

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/wesm/msgvault/internal/vector"
	"github.com/wesm/msgvault/internal/vector/embed"
)

// PipelineInputs is the parameter bundle for the pipeline runner.
type PipelineInputs struct {
	SampleIDs     []int64
	BackendBase   vector.Backend // must implement vector.BenchBackend; checked at runtime
	MainDB        *sql.DB
	VectorsDB     *sql.DB
	Client        embed.EmbeddingClient
	Preprocess    embed.PreprocessConfig
	MaxInputChars int
	BatchSize     int
	Workers       int
	Model         string
	Dimension     int
	// RunScope identifies this run inside the bench: fingerprint
	// (full form: "bench:<RunScope>:<Model>:<Dimension>"). The CLI
	// passes "<sweep_id>:<started_unix_nano>" — the run_id isn't
	// known until persistRunRow returns its LastInsertId, so the
	// nanos timestamp acts as the per-run uniqueness component.
	RunScope string
}

// PipelineResult carries the aggregator and the throwaway gen ID so
// the caller can record bench_runs.generation_id. The gen has already
// been dropped by the time RunPipeline returns; the ID is exposed
// purely for after-the-fact orphan auditability.
type PipelineResult struct {
	Aggregator   *Aggregator
	GenerationID vector.GenerationID
}

// RunPipeline drives N embed.Worker instances against a throwaway
// bench: generation. The throwaway gen is dropped on every return
// path (success, error, ctx cancel) via a defer with a fresh context
// and short timeout, so a stuck backend cannot hang the harness.
func RunPipeline(ctx context.Context, in PipelineInputs) (result *PipelineResult, err error) {
	bb, ok := in.BackendBase.(vector.BenchBackend)
	if !ok {
		return nil, fmt.Errorf("RunPipeline: backend does not support BenchBackend (pipeline-mode benchmarking unavailable)")
	}
	if in.Workers <= 0 {
		return nil, fmt.Errorf("RunPipeline: Workers must be > 0")
	}
	if in.BatchSize <= 0 {
		return nil, fmt.Errorf("RunPipeline: BatchSize must be > 0")
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
	// the run itself succeeded; otherwise the run's error is more
	// informative.
	defer func() {
		dropCtx, cancelDrop := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancelDrop()
		if dErr := bb.DropGeneration(dropCtx, gen); dErr != nil && err == nil {
			err = fmt.Errorf("RunPipeline: drop bench gen: %w", dErr)
		}
	}()

	agg := NewAggregator()
	agg.Start()
	defer agg.Stop()

	if err := seedSamplePending(ctx, in.VectorsDB, gen, in.SampleIDs); err != nil {
		return &PipelineResult{Aggregator: agg, GenerationID: gen}, fmt.Errorf("RunPipeline: seed pending: %w", err)
	}

	progress := make(chan embed.ProgressReport, in.Workers*4)
	done := make(chan error, in.Workers)
	var wg sync.WaitGroup
	for range in.Workers {
		wg.Go(func() {
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
			_, runErr := worker.RunOnce(ctx, gen)
			done <- runErr
		})
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
	wg.Wait() // belt-and-suspenders; done channel already drained
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
		r.ClaimElapsed, r.UpsertElapsed, r.CompleteElapsed, r.BatchTruncated)
}

// seedSamplePending inserts the given message IDs into
// pending_embeddings for the bench: gen. Uses INSERT OR IGNORE so a
// retry is safe.
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
