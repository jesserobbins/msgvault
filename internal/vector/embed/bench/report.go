package bench

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strings"
	"text/tabwriter"
	"time"
)

// RunSummary mirrors the bench_runs columns plus a few derived fields.
type RunSummary struct {
	ID           int64
	SweepID      sql.NullInt64
	StartedAt    int64
	FinishedAt   sql.NullInt64
	Status       string
	ErrorMessage sql.NullString
	Mode         string
	SampleName   string
	ConfigJSON   string
	ConfigHash   string
	CellJSON     string
	GenerationID sql.NullInt64

	MsgsTotal, MsgsSucceeded, MsgsTruncated, MsgsDropped    int64
	CharsTotal, ElapsedMs, EmbedMsSum                       int64
	MsgPerSec, UsPerChar, MsPerMsg                          float64
	BatchP50, BatchP95, BatchP99, BatchMax                  float64
	Errors4xx, Errors5xx, Errors429, ErrorsNetwork, Retries int64
	ClaimMsSum, UpsertMsSum, CompleteMsSum                  sql.NullInt64
}

// runSummaryJSON is a JSON-friendly mirror of RunSummary that flattens
// nullable types to plain Go pointers/strings for clean JSON output.
type runSummaryJSON struct {
	ID           int64   `json:"id"`
	SweepID      *int64  `json:"sweep_id,omitempty"`
	StartedAt    int64   `json:"started_at"`
	FinishedAt   *int64  `json:"finished_at,omitempty"`
	Status       string  `json:"status"`
	ErrorMessage *string `json:"error_message,omitempty"`
	Mode         string  `json:"mode"`
	SampleName   string  `json:"sample_name"`
	ConfigJSON   string  `json:"config_json"`
	ConfigHash   string  `json:"config_hash"`
	CellJSON     string  `json:"cell_json"`
	GenerationID *int64  `json:"generation_id,omitempty"`

	MsgsTotal     int64 `json:"msgs_total"`
	MsgsSucceeded int64 `json:"msgs_succeeded"`
	MsgsTruncated int64 `json:"msgs_truncated"`
	MsgsDropped   int64 `json:"msgs_dropped"`

	CharsTotal int64 `json:"chars_total"`
	ElapsedMs  int64 `json:"elapsed_ms"`
	EmbedMsSum int64 `json:"embed_ms_sum"`

	MsgPerSec float64 `json:"msg_per_sec"`
	UsPerChar float64 `json:"us_per_char"`
	MsPerMsg  float64 `json:"ms_per_msg"`

	BatchP50 float64 `json:"batch_p50_ms"`
	BatchP95 float64 `json:"batch_p95_ms"`
	BatchP99 float64 `json:"batch_p99_ms"`
	BatchMax float64 `json:"batch_max_ms"`

	Errors4xx     int64 `json:"errors_4xx"`
	Errors5xx     int64 `json:"errors_5xx"`
	Errors429     int64 `json:"errors_429"`
	ErrorsNetwork int64 `json:"errors_network"`
	Retries       int64 `json:"retries"`

	ClaimMsSum    *int64 `json:"claim_ms_sum,omitempty"`
	UpsertMsSum   *int64 `json:"upsert_ms_sum,omitempty"`
	CompleteMsSum *int64 `json:"complete_ms_sum,omitempty"`
}

func toJSONShape(r *RunSummary) runSummaryJSON {
	j := runSummaryJSON{
		ID:            r.ID,
		StartedAt:     r.StartedAt,
		Status:        r.Status,
		Mode:          r.Mode,
		SampleName:    r.SampleName,
		ConfigJSON:    r.ConfigJSON,
		ConfigHash:    r.ConfigHash,
		CellJSON:      r.CellJSON,
		MsgsTotal:     r.MsgsTotal,
		MsgsSucceeded: r.MsgsSucceeded,
		MsgsTruncated: r.MsgsTruncated,
		MsgsDropped:   r.MsgsDropped,
		CharsTotal:    r.CharsTotal,
		ElapsedMs:     r.ElapsedMs,
		EmbedMsSum:    r.EmbedMsSum,
		MsgPerSec:     r.MsgPerSec,
		UsPerChar:     r.UsPerChar,
		MsPerMsg:      r.MsPerMsg,
		BatchP50:      r.BatchP50,
		BatchP95:      r.BatchP95,
		BatchP99:      r.BatchP99,
		BatchMax:      r.BatchMax,
		Errors4xx:     r.Errors4xx,
		Errors5xx:     r.Errors5xx,
		Errors429:     r.Errors429,
		ErrorsNetwork: r.ErrorsNetwork,
		Retries:       r.Retries,
	}
	if r.SweepID.Valid {
		v := r.SweepID.Int64
		j.SweepID = &v
	}
	if r.FinishedAt.Valid {
		v := r.FinishedAt.Int64
		j.FinishedAt = &v
	}
	if r.ErrorMessage.Valid {
		v := r.ErrorMessage.String
		j.ErrorMessage = &v
	}
	if r.GenerationID.Valid {
		v := r.GenerationID.Int64
		j.GenerationID = &v
	}
	if r.ClaimMsSum.Valid {
		v := r.ClaimMsSum.Int64
		j.ClaimMsSum = &v
	}
	if r.UpsertMsSum.Valid {
		v := r.UpsertMsSum.Int64
		j.UpsertMsSum = &v
	}
	if r.CompleteMsSum.Valid {
		v := r.CompleteMsSum.Int64
		j.CompleteMsSum = &v
	}
	return j
}

const runsSelectSQL = `
SELECT id, sweep_id, started_at, finished_at, status, error_message, mode,
       sample_name, config_json, config_hash, cell_json, generation_id,
       msgs_total, msgs_succeeded, msgs_truncated, msgs_dropped,
       chars_total, elapsed_ms, embed_ms_sum,
       msg_per_sec, us_per_char, ms_per_msg,
       batch_p50_ms, batch_p95_ms, batch_p99_ms, batch_max_ms,
       errors_4xx, errors_5xx, errors_429, errors_network, retries,
       claim_ms_sum, upsert_ms_sum, complete_ms_sum
  FROM bench_runs`

func scanRunRow(scan func(...any) error) (*RunSummary, error) {
	var r RunSummary
	err := scan(
		&r.ID, &r.SweepID, &r.StartedAt, &r.FinishedAt, &r.Status, &r.ErrorMessage, &r.Mode,
		&r.SampleName, &r.ConfigJSON, &r.ConfigHash, &r.CellJSON, &r.GenerationID,
		&r.MsgsTotal, &r.MsgsSucceeded, &r.MsgsTruncated, &r.MsgsDropped,
		&r.CharsTotal, &r.ElapsedMs, &r.EmbedMsSum,
		&r.MsgPerSec, &r.UsPerChar, &r.MsPerMsg,
		&r.BatchP50, &r.BatchP95, &r.BatchP99, &r.BatchMax,
		&r.Errors4xx, &r.Errors5xx, &r.Errors429, &r.ErrorsNetwork, &r.Retries,
		&r.ClaimMsSum, &r.UpsertMsSum, &r.CompleteMsSum,
	)
	if err != nil {
		return nil, err
	}
	return &r, nil
}

// LoadRun fetches one bench_runs row by id.
func LoadRun(ctx context.Context, db *sql.DB, id int64) (*RunSummary, error) {
	if err := RequireSchema(ctx, db); err != nil {
		return nil, err
	}
	row := db.QueryRowContext(ctx, runsSelectSQL+" WHERE id = ?", id)
	return scanRunRow(row.Scan)
}

// LoadRuns fetches multiple bench_runs rows by id, in the order requested.
// Returns ErrNoBenchData if the bench tables are absent.
func LoadRuns(ctx context.Context, db *sql.DB, ids []int64) ([]*RunSummary, error) {
	if err := RequireSchema(ctx, db); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}
	// Fetch all into a map, then rebuild in request order.
	byID := make(map[int64]*RunSummary, len(ids))
	rows, err := db.QueryContext(ctx, runsSelectSQL+" WHERE id IN ("+placeholders(len(ids))+")", int64SliceToAny(ids)...)
	if err != nil {
		return nil, fmt.Errorf("LoadRuns: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		r, err := scanRunRow(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("LoadRuns scan: %w", err)
		}
		byID[r.ID] = r
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("LoadRuns rows: %w", err)
	}
	out := make([]*RunSummary, 0, len(ids))
	for _, id := range ids {
		if r, ok := byID[id]; ok {
			out = append(out, r)
		}
	}
	return out, nil
}

// ListRecentRuns fetches the N most recently started runs, optionally
// filtered to one sweep. Results are returned newest-first.
func ListRecentRuns(ctx context.Context, db *sql.DB, sweepID *int64, limit int) ([]*RunSummary, error) {
	if err := RequireSchema(ctx, db); err != nil {
		return nil, err
	}
	q := runsSelectSQL
	var args []any
	if sweepID != nil {
		q += " WHERE sweep_id = ?"
		args = append(args, *sweepID)
	}
	q += " ORDER BY started_at DESC, id DESC LIMIT ?"
	args = append(args, limit)

	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("ListRecentRuns: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []*RunSummary
	for rows.Next() {
		r, err := scanRunRow(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("ListRecentRuns scan: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// RenderShow prints a multi-line terminal block for one run.
func RenderShow(w io.Writer, run *RunSummary) error {
	sweepStr := "—"
	if run.SweepID.Valid {
		sweepStr = fmt.Sprintf("%d", run.SweepID.Int64)
	}
	startedStr := time.Unix(run.StartedAt, 0).UTC().Format(time.RFC3339)
	finishedStr := "—"
	if run.FinishedAt.Valid {
		finishedStr = time.Unix(run.FinishedAt.Int64, 0).UTC().Format(time.RFC3339)
	}

	fmt.Fprintf(w, "Run %d  sweep=%s  status=%s\n", run.ID, sweepStr, run.Status)
	fmt.Fprintf(w, "  mode=%-10s  sample=%s\n", run.Mode, run.SampleName)
	fmt.Fprintf(w, "  started=%-20s  finished=%s\n", startedStr, finishedStr)

	if run.ErrorMessage.Valid && run.ErrorMessage.String != "" {
		fmt.Fprintf(w, "  error: %s\n", run.ErrorMessage.String)
	}

	fmt.Fprintf(w, "\n  Throughput\n")
	fmt.Fprintf(w, "    msg/s=%.2f  µs/char=%.2f  ms/msg=%.2f\n",
		run.MsgPerSec, run.UsPerChar, run.MsPerMsg)

	fmt.Fprintf(w, "\n  Messages\n")
	fmt.Fprintf(w, "    total=%-8d  succeeded=%-8d  truncated=%-8d  dropped=%d\n",
		run.MsgsTotal, run.MsgsSucceeded, run.MsgsTruncated, run.MsgsDropped)

	fmt.Fprintf(w, "\n  Batch latency (ms)\n")
	fmt.Fprintf(w, "    p50=%-8.1f  p95=%-8.1f  p99=%-8.1f  max=%.1f\n",
		run.BatchP50, run.BatchP95, run.BatchP99, run.BatchMax)

	fmt.Fprintf(w, "\n  Errors\n")
	fmt.Fprintf(w, "    4xx=%-6d  5xx=%-6d  429=%-6d  network=%-6d  retries=%d\n",
		run.Errors4xx, run.Errors5xx, run.Errors429, run.ErrorsNetwork, run.Retries)

	fmt.Fprintln(w)
	return nil
}

// RenderShowJSON encodes a single run as JSON.
func RenderShowJSON(w io.Writer, run *RunSummary) error {
	return json.NewEncoder(w).Encode(toJSONShape(run))
}

// RenderList prints a one-line-per-run table sorted by the input slice's order.
func RenderList(w io.Writer, runs []*RunSummary) error {
	tw := tabwriter.NewWriter(w, 2, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tSweep\tStatus\tMode\tSample\tMsgs\tmsg/s\tµs/char\tp50ms\tp95ms")
	for _, r := range runs {
		sweepStr := "—"
		if r.SweepID.Valid {
			sweepStr = fmt.Sprintf("%d", r.SweepID.Int64)
		}
		fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%s\t%d\t%.2f\t%.2f\t%.1f\t%.1f\n",
			r.ID, sweepStr, r.Status, r.Mode, r.SampleName,
			r.MsgsSucceeded, r.MsgPerSec, r.UsPerChar,
			r.BatchP50, r.BatchP95)
	}
	return tw.Flush()
}

// RenderListJSON encodes a list of runs as JSON.
func RenderListJSON(w io.Writer, runs []*RunSummary) error {
	shapes := make([]runSummaryJSON, len(runs))
	for i, r := range runs {
		shapes[i] = toJSONShape(r)
	}
	return json.NewEncoder(w).Encode(shapes)
}

// numericMetrics is the ordered set of metrics compared in RenderCompare.
var numericMetrics = []struct {
	name string
	get  func(*RunSummary) float64
}{
	{"msg_per_sec", func(r *RunSummary) float64 { return r.MsgPerSec }},
	{"us_per_char", func(r *RunSummary) float64 { return r.UsPerChar }},
	{"ms_per_msg", func(r *RunSummary) float64 { return r.MsPerMsg }},
	{"batch_p50_ms", func(r *RunSummary) float64 { return r.BatchP50 }},
	{"batch_p95_ms", func(r *RunSummary) float64 { return r.BatchP95 }},
	{"batch_p99_ms", func(r *RunSummary) float64 { return r.BatchP99 }},
	{"batch_max_ms", func(r *RunSummary) float64 { return r.BatchMax }},
	{"msgs_total", func(r *RunSummary) float64 { return float64(r.MsgsTotal) }},
	{"msgs_succeeded", func(r *RunSummary) float64 { return float64(r.MsgsSucceeded) }},
	{"elapsed_ms", func(r *RunSummary) float64 { return float64(r.ElapsedMs) }},
	{"errors_4xx", func(r *RunSummary) float64 { return float64(r.Errors4xx) }},
	{"errors_5xx", func(r *RunSummary) float64 { return float64(r.Errors5xx) }},
	{"retries", func(r *RunSummary) float64 { return float64(r.Retries) }},
}

// RenderCompare prints a side-by-side metric table for the given runs.
// Differences ≥5% relative to the first run are marked with a "*".
func RenderCompare(w io.Writer, runs []*RunSummary) error {
	if len(runs) == 0 {
		fmt.Fprintln(w, "(no runs)")
		return nil
	}

	tw := tabwriter.NewWriter(w, 2, 0, 2, ' ', 0)

	// Header row: metric name then one column per run id.
	var header strings.Builder
	header.WriteString("metric")
	for _, r := range runs {
		fmt.Fprintf(&header, "\t%d", r.ID)
	}
	fmt.Fprintln(tw, header.String())

	for _, m := range numericMetrics {
		var line strings.Builder
		line.WriteString(m.name)
		baseline := m.get(runs[0])
		for i, r := range runs {
			val := m.get(r)
			if i == 0 {
				fmt.Fprintf(&line, "\t%.2f", val)
			} else {
				line.WriteByte('\t')
				line.WriteString(formatWithDelta(val, baseline))
			}
		}
		fmt.Fprintln(tw, line.String())
	}

	return tw.Flush()
}

// RenderCompareJSON encodes a list of runs as JSON (same shape as RenderListJSON).
func RenderCompareJSON(w io.Writer, runs []*RunSummary) error {
	return RenderListJSON(w, runs)
}

// formatWithDelta formats value; appends "*" if the relative delta from
// baseline is ≥5%.
func formatWithDelta(value, baseline float64) string {
	if baseline == 0 || math.IsNaN(baseline) || math.IsNaN(value) {
		return fmt.Sprintf("%.2f", value)
	}
	delta := math.Abs(value-baseline) / math.Abs(baseline)
	if delta >= 0.05 {
		return fmt.Sprintf("%.2f*", value)
	}
	return fmt.Sprintf("%.2f", value)
}

// placeholders returns n comma-separated "?" tokens.
func placeholders(n int) string {
	if n == 0 {
		return ""
	}
	b := make([]byte, 2*n-1)
	for i := range b {
		if i%2 == 0 {
			b[i] = '?'
		} else {
			b[i] = ','
		}
	}
	return string(b)
}

func int64SliceToAny(ids []int64) []any {
	out := make([]any, len(ids))
	for i, id := range ids {
		out[i] = id
	}
	return out
}
