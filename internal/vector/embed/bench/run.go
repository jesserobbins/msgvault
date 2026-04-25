//go:build sqlite_vec

package bench

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/wesm/msgvault/internal/mime"
	"github.com/wesm/msgvault/internal/vector"
	"github.com/wesm/msgvault/internal/vector/embed"
)

// RunCellInputs bundles per-cell parameters.
type RunCellInputs struct {
	SweepID       *int64 // nil for ad-hoc `run`
	SampleName    string
	Mode          string // "endpoint" | "pipeline"
	Cell          Cell   // matrix coordinates
	BackendBase   vector.Backend
	MainDB        *sql.DB
	VectorsDB     *sql.DB
	Client        embed.EmbeddingClient
	Preprocess    embed.PreprocessConfig
	MaxInputChars int
	BatchSize     int
	Workers       int
	WarmupBatches int
	Model         string
	Dimension     int
}

// RunCell executes one matrix cell and persists exactly one
// bench_runs row. Returns the inserted row id and a non-nil error
// only when the row could not be persisted; per-batch failures are
// recorded in the row's error counters and `status='completed'`.
func RunCell(ctx context.Context, in RunCellInputs) (int64, error) {
	// Use a fresh context for all persistence so a cancelled ctx
	// still writes the bench_runs row.
	persistCtx, cancelPersist := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelPersist()

	if err := EnsureSchema(persistCtx, in.VectorsDB); err != nil {
		return 0, err
	}
	started := time.Now()

	// Build resolved config for hashing.
	resolved := map[string]any{
		"model":                       in.Model,
		"dimension":                   in.Dimension,
		"batch_size":                  in.BatchSize,
		"workers":                     in.Workers,
		"warmup_batches":              in.WarmupBatches,
		"max_input_chars":             in.MaxInputChars,
		"preprocess.strip_quotes":     in.Preprocess.StripQuotes,
		"preprocess.strip_signatures": in.Preprocess.StripSignatures,
		"mode":                        in.Mode,
	}
	cfgJSON, err := MarshalCanonical(resolved)
	if err != nil {
		return 0, fmt.Errorf("RunCell: canonicalize config: %w", err)
	}
	cfgHash, err := ConfigHash(resolved)
	if err != nil {
		return 0, fmt.Errorf("RunCell: hash config: %w", err)
	}
	cellJSON, err := MarshalCanonical(map[string]any(in.Cell))
	if err != nil {
		return 0, fmt.Errorf("RunCell: marshal cell: %w", err)
	}

	sampleIDs, err := SampleMessageIDs(persistCtx, in.VectorsDB, in.SampleName)
	if err != nil {
		return 0, fmt.Errorf("RunCell: load sample: %w", err)
	}

	var (
		agg         *Aggregator
		msgsDropped int
		runErr      error
		genID       sql.NullInt64
		runStatus   = "completed"
	)

	switch in.Mode {
	case "endpoint":
		prepared, dropped, prepErr := loadAndPreprocess(ctx, in.MainDB, sampleIDs, in.Preprocess, in.MaxInputChars)
		if prepErr != nil {
			// Fold preprocess error into the unified classification below.
			runErr = prepErr
			agg = NewAggregator()
		} else {
			msgsDropped = dropped
			a, endpointErr := RunEndpoint(ctx, EndpointInputs{
				Messages:      prepared,
				Client:        in.Client,
				BatchSize:     in.BatchSize,
				Workers:       in.Workers,
				WarmupBatches: in.WarmupBatches,
			})
			agg = a
			runErr = endpointErr
		}
	case "pipeline":
		if _, ok := in.BackendBase.(vector.BenchBackend); !ok {
			agg = NewAggregator()
			errMsg := "backend does not implement BenchBackend (pipeline-mode benchmarking unavailable)"
			return persistRunRow(persistCtx, in.VectorsDB, in, started, agg, len(sampleIDs), 0, "error", errMsg, genID, cfgJSON, cfgHash, cellJSON)
		}
		result, pipeErr := RunPipeline(ctx, PipelineInputs{
			SampleIDs:     sampleIDs,
			BackendBase:   in.BackendBase,
			MainDB:        in.MainDB,
			VectorsDB:     in.VectorsDB,
			Client:        in.Client,
			Preprocess:    in.Preprocess,
			MaxInputChars: in.MaxInputChars,
			BatchSize:     in.BatchSize,
			Workers:       in.Workers,
			Model:         in.Model,
			Dimension:     in.Dimension,
			RunScope:      formatRunScope(in.SweepID, started),
		})
		if result != nil {
			agg = result.Aggregator
			if result.GenerationID != 0 {
				genID = sql.NullInt64{Int64: int64(result.GenerationID), Valid: true}
			}
		} else {
			agg = NewAggregator()
		}
		runErr = pipeErr
	default:
		return 0, fmt.Errorf("RunCell: unknown mode %q", in.Mode)
	}

	var errMsg string
	switch {
	case errors.Is(runErr, context.Canceled):
		runStatus = "aborted"
		errMsg = "context canceled"
	case errors.Is(runErr, context.DeadlineExceeded):
		runStatus = "aborted"
		errMsg = "context deadline exceeded"
	case runErr != nil:
		runStatus = "error"
		errMsg = runErr.Error()
	}

	return persistRunRow(persistCtx, in.VectorsDB, in, started, agg, len(sampleIDs), msgsDropped, runStatus, errMsg, genID, cfgJSON, cfgHash, cellJSON)
}

// persistRunRow inserts a single bench_runs row with the given
// outcome. Returns (run_id, error). Caller should not return runErr
// — the run-level error has already been folded into status/error_message.
//
// msgsTotal is the size of the input sample (i.e. len(sampleIDs)) so
// it captures messages that were attempted but never reached
// embedding (preprocess drops, fetch failures, ctx-cancelled before
// embed). msgs_succeeded comes from the aggregator and counts only
// what actually embedded.
func persistRunRow(ctx context.Context, db *sql.DB, in RunCellInputs, started time.Time, agg *Aggregator, msgsTotal, msgsDropped int, status, errMsg string, genID sql.NullInt64, cfgJSON []byte, cfgHash string, cellJSON []byte) (int64, error) {
	r := agg.Result()
	var sweepID sql.NullInt64
	if in.SweepID != nil {
		sweepID = sql.NullInt64{Int64: *in.SweepID, Valid: true}
	}
	var errMsgNS sql.NullString
	if errMsg != "" {
		errMsgNS = sql.NullString{String: errMsg, Valid: true}
	}
	finished := time.Now().Unix()
	res, err := db.ExecContext(ctx, `
		INSERT INTO bench_runs (
			sweep_id, started_at, finished_at, status, error_message, mode,
			sample_name, config_json, config_hash, cell_json, generation_id,
			msgs_total, msgs_succeeded, msgs_truncated, msgs_dropped,
			chars_total, elapsed_ms, embed_ms_sum,
			msg_per_sec, us_per_char, ms_per_msg,
			batch_p50_ms, batch_p95_ms, batch_p99_ms, batch_max_ms,
			errors_4xx, errors_5xx, errors_429, errors_network, retries,
			claim_ms_sum, upsert_ms_sum, complete_ms_sum
		) VALUES (
			?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?,
			?, ?, ?, ?,
			?, ?, ?,
			?, ?, ?,
			?, ?, ?, ?,
			?, ?, ?, ?, ?,
			?, ?, ?
		)`,
		sweepID, started.Unix(), finished, status, errMsgNS, in.Mode,
		in.SampleName, string(cfgJSON), cfgHash, string(cellJSON), genID,
		msgsTotal, r.Msgs, r.Truncated, msgsDropped,
		r.Chars, r.ElapsedMs, r.EmbedMsSum,
		r.MsgPerSec, r.UsPerChar, r.MsPerMsg,
		nanToZero(r.BatchP50), nanToZero(r.BatchP95), nanToZero(r.BatchP99), nanToZero(r.BatchMax),
		r.Errors4xx, r.Errors5xx, r.Errors429, r.ErrorsNet, r.Retries,
		r.ClaimMs, r.UpsertMs, r.CompleteMs,
	)
	if err != nil {
		return 0, fmt.Errorf("persistRunRow: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	return id, nil
}

func nanToZero(f float64) float64 {
	if f != f { // NaN check
		return 0
	}
	return f
}

// formatRunScope produces the per-run portion of a bench: fingerprint
// as "<sweep_id>:<started_unix_nano>". The run_id can't be used here
// because it isn't issued until persistRunRow returns its
// LastInsertId, so the nanosecond timestamp acts as the uniqueness
// component within a sweep.
func formatRunScope(sweepID *int64, started time.Time) string {
	sw := int64(0)
	if sweepID != nil {
		sw = *sweepID
	}
	return fmt.Sprintf("%d:%d", sw, started.UnixNano())
}

// loadAndPreprocess fetches subjects/bodies for the given sample IDs
// and runs embed.Preprocess on each. Empty results increment the
// dropped counter and are excluded from the returned slice.
func loadAndPreprocess(ctx context.Context, db *sql.DB, ids []int64, pp embed.PreprocessConfig, maxChars int) ([]PreparedMessage, int, error) {
	if len(ids) == 0 {
		return nil, 0, nil
	}
	placeholders := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		placeholders[i] = "?"
		args[i] = id
	}
	q := fmt.Sprintf(`
		SELECT m.id, COALESCE(m.subject, ''), COALESCE(mb.body_text, ''), COALESCE(mb.body_html, '')
		  FROM messages m
		  LEFT JOIN message_bodies mb ON mb.message_id = m.id
		 WHERE m.id IN (%s)`, strings.Join(placeholders, ","))
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("loadAndPreprocess: query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []PreparedMessage
	dropped := 0
	for rows.Next() {
		var id int64
		var subject, bodyText, bodyHTML string
		if err := rows.Scan(&id, &subject, &bodyText, &bodyHTML); err != nil {
			return nil, 0, err
		}
		body := bodyText
		if body == "" && bodyHTML != "" {
			body = mime.StripHTML(bodyHTML)
		}
		text, trunc := embed.Preprocess(subject, body, maxChars, pp)
		if strings.TrimSpace(text) == "" {
			dropped++
			continue
		}
		out = append(out, PreparedMessage{
			ID:    id,
			Text:  text,
			Chars: utf8.RuneCountInString(text),
			Trunc: trunc,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return out, dropped, nil
}

// RunSweep iterates plan cells (typically from plan.Cells()),
// invoking RunCell for each. Pipeline-mode cells run strictly
// sequentially — the unique partial index on state='building' allows
// only one bench gen at a time. Endpoint-mode cells also run
// sequentially in this implementation; concurrency may be added
// behind a flag later.
//
// `planTOML` is the raw plan source (or a synthesized version for
// flags-only invocations) — it is stored verbatim in
// bench_sweeps.plan_toml. `planPath` records the file path or "auto"
// path; pass empty string when the source has no on-disk artifact.
//
// Per-cell collaborators (mainDB, backend, embedding client) are
// supplied by the cellRunner closure, not by RunSweep — different
// cells may target different endpoints, models, or even backends.
func RunSweep(ctx context.Context, vecDB *sql.DB,
	plan *Plan, sampleName string, planTOML string, planPath string, gitSHA string, notes string,
	cellRunner func(ctx context.Context, sweepID int64, cell Cell) (RunCellInputs, error),
) (int64, error) {
	// Use a fresh context for sweep bookkeeping writes so a cancel
	// mid-sweep still finalizes the bench_sweeps row.
	persistCtx, cancelPersist := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelPersist()

	if err := EnsureSchema(persistCtx, vecDB); err != nil {
		return 0, err
	}
	var planPathNS sql.NullString
	if planPath != "" {
		planPathNS = sql.NullString{String: planPath, Valid: true}
	}
	var gitSHANS sql.NullString
	if gitSHA != "" {
		gitSHANS = sql.NullString{String: gitSHA, Valid: true}
	}
	started := time.Now().Unix()
	res, err := vecDB.ExecContext(persistCtx, `
		INSERT INTO bench_sweeps (started_at, status, plan_path, plan_toml, git_sha, notes)
		VALUES (?, 'running', ?, ?, ?, ?)`,
		started, planPathNS, planTOML, gitSHANS, notes)
	if err != nil {
		return 0, fmt.Errorf("RunSweep: insert sweep: %w", err)
	}
	sweepID, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}

	cells, err := plan.Cells()
	if err != nil {
		_ = finalizeSweep(persistCtx, vecDB, sweepID, "error")
		return sweepID, err
	}

	for _, cell := range cells {
		if ctx.Err() != nil {
			_ = finalizeSweep(persistCtx, vecDB, sweepID, "aborted")
			return sweepID, ctx.Err()
		}
		in, err := cellRunner(ctx, sweepID, cell)
		if err != nil {
			_ = finalizeSweep(persistCtx, vecDB, sweepID, "error")
			return sweepID, err
		}
		if _, err := RunCell(ctx, in); err != nil {
			_ = finalizeSweep(persistCtx, vecDB, sweepID, "error")
			return sweepID, err
		}
	}

	if err := finalizeSweep(persistCtx, vecDB, sweepID, "completed"); err != nil {
		return sweepID, err
	}
	return sweepID, nil
}

func finalizeSweep(ctx context.Context, db *sql.DB, sweepID int64, status string) error {
	_, err := db.ExecContext(ctx,
		`UPDATE bench_sweeps SET finished_at = ?, status = ? WHERE id = ?`,
		time.Now().Unix(), status, sweepID)
	return err
}
