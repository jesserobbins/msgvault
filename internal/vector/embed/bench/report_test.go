package bench

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

// insertRun inserts a bench_runs row with the given field overrides and
// returns the inserted id. All fields not explicitly overridden are set
// to sensible defaults.
func insertRun(t *testing.T, db *sql.DB, sweepID *int64, msgPerSec float64) int64 {
	t.Helper()
	var swID sql.NullInt64
	if sweepID != nil {
		swID = sql.NullInt64{Int64: *sweepID, Valid: true}
	}
	res, err := db.Exec(`
		INSERT INTO bench_runs (
			sweep_id, started_at, finished_at, status, mode,
			sample_name, config_json, config_hash, cell_json,
			msgs_total, msgs_succeeded, msgs_truncated, msgs_dropped,
			chars_total, elapsed_ms, embed_ms_sum,
			msg_per_sec, us_per_char, ms_per_msg,
			batch_p50_ms, batch_p95_ms, batch_p99_ms, batch_max_ms,
			errors_4xx, errors_5xx, errors_429, errors_network, retries
		) VALUES (
			?, 1700000000, 1700001000, 'completed', 'endpoint',
			'sample-a', '{}', 'deadbeef', '{}',
			100, 98, 2, 1,
			50000, 5000, 4800,
			?, 1.5, 10.2,
			120.0, 200.0, 250.0, 300.0,
			0, 0, 0, 0, 0
		)`, swID, msgPerSec)
	if err != nil {
		t.Fatalf("insertRun: %v", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("insertRun LastInsertId: %v", err)
	}
	return id
}

func TestLoadRun_Basic(t *testing.T) {
	db := openMem(t)
	ctx := context.Background()
	if err := EnsureSchema(ctx, db); err != nil {
		t.Fatal(err)
	}
	id := insertRun(t, db, nil, 42.0)
	run, err := LoadRun(ctx, db, id)
	if err != nil {
		t.Fatalf("LoadRun: %v", err)
	}
	if run.ID != id {
		t.Errorf("ID: got %d, want %d", run.ID, id)
	}
	if run.Status != "completed" {
		t.Errorf("Status: got %q, want completed", run.Status)
	}
	if run.Mode != "endpoint" {
		t.Errorf("Mode: got %q, want endpoint", run.Mode)
	}
	if run.SampleName != "sample-a" {
		t.Errorf("SampleName: got %q, want sample-a", run.SampleName)
	}
	if run.MsgPerSec != 42.0 {
		t.Errorf("MsgPerSec: got %f, want 42.0", run.MsgPerSec)
	}
	if run.BatchP50 != 120.0 {
		t.Errorf("BatchP50: got %f, want 120.0", run.BatchP50)
	}
	if run.BatchP95 != 200.0 {
		t.Errorf("BatchP95: got %f, want 200.0", run.BatchP95)
	}
}

func TestLoadRun_NotFound(t *testing.T) {
	db := openMem(t)
	ctx := context.Background()
	if err := EnsureSchema(ctx, db); err != nil {
		t.Fatal(err)
	}
	_, err := LoadRun(ctx, db, 9999)
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows, got %v", err)
	}
}

func TestListRecentRuns_OrderAndLimit(t *testing.T) {
	db := openMem(t)
	ctx := context.Background()
	if err := EnsureSchema(ctx, db); err != nil {
		t.Fatal(err)
	}
	// Insert three rows; the AUTOINCREMENT id proxy for started_at ordering.
	id1 := insertRun(t, db, nil, 10.0)
	id2 := insertRun(t, db, nil, 20.0)
	id3 := insertRun(t, db, nil, 30.0)

	runs, err := ListRecentRuns(ctx, db, nil, 2)
	if err != nil {
		t.Fatalf("ListRecentRuns: %v", err)
	}
	if len(runs) != 2 {
		t.Fatalf("len: got %d, want 2", len(runs))
	}
	// Expect most-recent first: id3, id2 (id1 excluded by limit).
	if runs[0].ID != id3 {
		t.Errorf("runs[0].ID: got %d, want %d", runs[0].ID, id3)
	}
	if runs[1].ID != id2 {
		t.Errorf("runs[1].ID: got %d, want %d", runs[1].ID, id2)
	}
	_ = id1 // not expected in result
}

func TestListRecentRuns_FilteredBySweep(t *testing.T) {
	db := openMem(t)
	ctx := context.Background()
	if err := EnsureSchema(ctx, db); err != nil {
		t.Fatal(err)
	}
	// Insert a sweep row to satisfy FK.
	res, err := db.Exec(`INSERT INTO bench_sweeps (started_at, status, plan_toml) VALUES (1, 'completed', 'x')`)
	if err != nil {
		t.Fatal(err)
	}
	sweepID, _ := res.LastInsertId()

	id1 := insertRun(t, db, &sweepID, 10.0)
	_id2 := insertRun(t, db, nil, 20.0) // no sweep

	runs, err := ListRecentRuns(ctx, db, &sweepID, 10)
	if err != nil {
		t.Fatalf("ListRecentRuns sweep filter: %v", err)
	}
	if len(runs) != 1 {
		t.Fatalf("len: got %d, want 1", len(runs))
	}
	if runs[0].ID != id1 {
		t.Errorf("runs[0].ID: got %d, want %d", runs[0].ID, id1)
	}
	_ = _id2
}

func TestRenderShow_TerminalContainsExpectedFields(t *testing.T) {
	db := openMem(t)
	ctx := context.Background()
	if err := EnsureSchema(ctx, db); err != nil {
		t.Fatal(err)
	}
	id := insertRun(t, db, nil, 55.5)
	run, err := LoadRun(ctx, db, id)
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := RenderShow(&buf, run); err != nil {
		t.Fatalf("RenderShow: %v", err)
	}
	out := buf.String()

	for _, want := range []string{
		"completed", // status
		"endpoint",  // mode
		"sample-a",  // sample name
		"55.5",      // msg_per_sec
		"120.0",     // batch_p50
		"200.0",     // batch_p95
	} {
		if !strings.Contains(out, want) {
			t.Errorf("RenderShow output missing %q\nfull output:\n%s", want, out)
		}
	}
}

func TestRenderShowJSON_Shape(t *testing.T) {
	db := openMem(t)
	ctx := context.Background()
	if err := EnsureSchema(ctx, db); err != nil {
		t.Fatal(err)
	}
	id := insertRun(t, db, nil, 77.7)
	run, err := LoadRun(ctx, db, id)
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := RenderShowJSON(&buf, run); err != nil {
		t.Fatalf("RenderShowJSON: %v", err)
	}

	// Round-trip check: the JSON must parse cleanly and preserve key fields.
	var got map[string]any
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("json.Unmarshal: %v\nraw: %s", err, buf.String())
	}
	// Check a few critical fields.
	if got["status"] != "completed" {
		t.Errorf("status: got %v", got["status"])
	}
	if got["mode"] != "endpoint" {
		t.Errorf("mode: got %v", got["mode"])
	}
	// msg_per_sec is stored as float; json decodes numbers as float64.
	if v, ok := got["msg_per_sec"].(float64); !ok || v != 77.7 {
		t.Errorf("msg_per_sec: got %v (%T)", got["msg_per_sec"], got["msg_per_sec"])
	}
}

func TestRenderList_TerminalContainsAllRuns(t *testing.T) {
	db := openMem(t)
	ctx := context.Background()
	if err := EnsureSchema(ctx, db); err != nil {
		t.Fatal(err)
	}
	id1 := insertRun(t, db, nil, 10.0)
	id2 := insertRun(t, db, nil, 20.0)
	id3 := insertRun(t, db, nil, 30.0)

	runs, err := ListRecentRuns(ctx, db, nil, 10)
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := RenderList(&buf, runs); err != nil {
		t.Fatalf("RenderList: %v", err)
	}
	out := buf.String()
	for _, id := range []int64{id1, id2, id3} {
		s := strings.Contains(out, strings.TrimSpace(strings.Repeat(" ", 0)+string(rune('0'+id))))
		// More robust: just check the numeric id appears.
		idStr := itoa(id)
		if !strings.Contains(out, idStr) {
			_ = s
			t.Errorf("RenderList missing run id %d\noutput:\n%s", id, out)
		}
	}
}

func TestRenderCompare_HighlightsDeltas(t *testing.T) {
	db := openMem(t)
	ctx := context.Background()
	if err := EnsureSchema(ctx, db); err != nil {
		t.Fatal(err)
	}
	// Run 0: baseline 100 msg/s. Run 1: 120 msg/s (20% delta → should get "*").
	id1 := insertRun(t, db, nil, 100.0)
	id2 := insertRun(t, db, nil, 120.0)

	run1, err := LoadRun(ctx, db, id1)
	if err != nil {
		t.Fatal(err)
	}
	run2, err := LoadRun(ctx, db, id2)
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := RenderCompare(&buf, []*RunSummary{run1, run2}); err != nil {
		t.Fatalf("RenderCompare: %v", err)
	}
	out := buf.String()

	// The second run's msg_per_sec value (120.00) should be marked with *.
	if !strings.Contains(out, "120.00*") {
		t.Errorf("RenderCompare: expected 120.00* in output for 20%% delta\noutput:\n%s", out)
	}
	// The baseline (100.00) should NOT have a *.
	if strings.Contains(out, "100.00*") {
		t.Errorf("RenderCompare: baseline value 100.00 should not be marked with *\noutput:\n%s", out)
	}
}

// itoa converts int64 to decimal string without importing strconv/fmt in test
// helpers — inline to keep the test file free of extra imports.
func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
