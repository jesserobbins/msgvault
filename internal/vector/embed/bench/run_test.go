//go:build sqlite_vec

package bench

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"github.com/wesm/msgvault/internal/vector"
	"github.com/wesm/msgvault/internal/vector/embed"
	"github.com/wesm/msgvault/internal/vector/sqlitevec"
)

// orchFixture wires real sqlitevec backend + real main DB with N
// messages.
type orchFixture struct {
	Backend   *sqlitevec.Backend
	MainDB    *sql.DB
	VectorsDB *sql.DB
	Client    *fakeEmbedClient
}

func newOrchFixture(t *testing.T, n int) *orchFixture {
	t.Helper()
	if err := sqlitevec.RegisterExtension(); err != nil {
		t.Fatalf("RegisterExtension: %v", err)
	}
	dir := t.TempDir()
	mainPath := filepath.Join(dir, "main.db")
	mainDB, err := sql.Open(sqlitevec.DriverName(), mainPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mainDB.Close() })
	if _, err := mainDB.Exec(`
CREATE TABLE messages (id INTEGER PRIMARY KEY, subject TEXT, deleted_from_source_at DATETIME);
CREATE TABLE message_bodies (message_id INTEGER PRIMARY KEY, body_text TEXT, body_html TEXT);`); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= n; i++ {
		if _, err := mainDB.Exec(`INSERT INTO messages (id, subject) VALUES (?, ?)`, i, "subj"); err != nil {
			t.Fatal(err)
		}
		if _, err := mainDB.Exec(`INSERT INTO message_bodies (message_id, body_text) VALUES (?, ?)`, i, "body"); err != nil {
			t.Fatal(err)
		}
	}
	vecPath := filepath.Join(dir, "vectors.db")
	b, err := sqlitevec.Open(context.Background(), sqlitevec.Options{Path: vecPath, MainPath: mainPath, Dimension: 4, MainDB: mainDB})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	vecDB, err := sql.Open(sqlitevec.DriverName(), vecPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = vecDB.Close() })
	if err := EnsureSchema(context.Background(), vecDB); err != nil {
		t.Fatal(err)
	}
	return &orchFixture{Backend: b, MainDB: mainDB, VectorsDB: vecDB, Client: &fakeEmbedClient{dim: 4}}
}

func TestRunCell_Endpoint_HappyPath(t *testing.T) {
	fx := newOrchFixture(t, 5)
	// Create a sample with all 5 message IDs.
	ids := []int64{1, 2, 3, 4, 5}
	if err := CreateSampleFromIDs(context.Background(), fx.VectorsDB, "demo", ids, SampleMeta{Seed: 1}); err != nil {
		t.Fatal(err)
	}
	runID, err := RunCell(context.Background(), RunCellInputs{
		SampleName:    "demo",
		Mode:          "endpoint",
		Cell:          Cell{"endpoint": "http://x", "model": "m", "batch_size": int64(2), "workers": int64(1)},
		BackendBase:   fx.Backend,
		MainDB:        fx.MainDB,
		VectorsDB:     fx.VectorsDB,
		Client:        fx.Client,
		BatchSize:     2,
		Workers:       1,
		WarmupBatches: 0,
		MaxInputChars: 1000,
		Model:         "m",
		Dimension:     4,
	})
	if err != nil {
		t.Fatalf("RunCell: %v", err)
	}
	var status, errMsg sql.NullString
	var msgs sql.NullInt64
	var genID sql.NullInt64
	if err := fx.VectorsDB.QueryRow(`SELECT status, error_message, msgs_succeeded, generation_id FROM bench_runs WHERE id = ?`, runID).Scan(&status, &errMsg, &msgs, &genID); err != nil {
		t.Fatal(err)
	}
	if status.String != "completed" {
		t.Errorf("status: got %q, want completed", status.String)
	}
	if errMsg.Valid && errMsg.String != "" {
		t.Errorf("error_message should be NULL, got %q", errMsg.String)
	}
	if msgs.Int64 != 5 {
		t.Errorf("msgs_succeeded: got %d, want 5", msgs.Int64)
	}
	if genID.Valid {
		t.Errorf("generation_id should be NULL in endpoint mode, got %d", genID.Int64)
	}
}

func TestRunCell_Endpoint_AbortedOnCancel(t *testing.T) {
	fx := newOrchFixture(t, 3)
	if err := CreateSampleFromIDs(context.Background(), fx.VectorsDB, "demo", []int64{1, 2, 3}, SampleMeta{}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	runID, err := RunCell(ctx, RunCellInputs{
		SampleName:    "demo",
		Mode:          "endpoint",
		Cell:          Cell{},
		BackendBase:   fx.Backend,
		MainDB:        fx.MainDB,
		VectorsDB:     fx.VectorsDB,
		Client:        fx.Client,
		BatchSize:     1,
		Workers:       1,
		MaxInputChars: 1000,
		Model:         "m",
		Dimension:     4,
	})
	if err != nil {
		t.Fatalf("RunCell should persist row even on cancel: %v", err)
	}
	var status, errMsg sql.NullString
	if err := fx.VectorsDB.QueryRow(`SELECT status, error_message FROM bench_runs WHERE id = ?`, runID).Scan(&status, &errMsg); err != nil {
		t.Fatal(err)
	}
	if status.String != "aborted" {
		t.Errorf("status: got %q, want aborted", status.String)
	}
	if !errMsg.Valid || !strings.Contains(errMsg.String, "context") {
		t.Errorf("error_message should mention context; got %q", errMsg.String)
	}
}

func TestRunCell_Pipeline_PopulatesGenerationID(t *testing.T) {
	fx := newOrchFixture(t, 3)
	if err := CreateSampleFromIDs(context.Background(), fx.VectorsDB, "demo", []int64{1, 2, 3}, SampleMeta{}); err != nil {
		t.Fatal(err)
	}
	runID, err := RunCell(context.Background(), RunCellInputs{
		SampleName:    "demo",
		Mode:          "pipeline",
		Cell:          Cell{},
		BackendBase:   fx.Backend,
		MainDB:        fx.MainDB,
		VectorsDB:     fx.VectorsDB,
		Client:        fx.Client,
		BatchSize:     2,
		Workers:       1,
		MaxInputChars: 1000,
		Model:         "fake",
		Dimension:     4,
	})
	if err != nil {
		t.Fatalf("RunCell pipeline: %v", err)
	}
	var genID sql.NullInt64
	if err := fx.VectorsDB.QueryRow(`SELECT generation_id FROM bench_runs WHERE id = ?`, runID).Scan(&genID); err != nil {
		t.Fatal(err)
	}
	if !genID.Valid {
		t.Errorf("pipeline-mode run should populate generation_id")
	}
}

func TestRunCell_Pipeline_RejectsNonBenchBackend(t *testing.T) {
	fx := newOrchFixture(t, 3)
	if err := CreateSampleFromIDs(context.Background(), fx.VectorsDB, "demo", []int64{1, 2, 3}, SampleMeta{}); err != nil {
		t.Fatal(err)
	}
	runID, err := RunCell(context.Background(), RunCellInputs{
		SampleName:    "demo",
		Mode:          "pipeline",
		Cell:          Cell{},
		BackendBase:   stubBackend{}, // doesn't implement BenchBackend
		MainDB:        fx.MainDB,
		VectorsDB:     fx.VectorsDB,
		Client:        fx.Client,
		BatchSize:     1,
		Workers:       1,
		MaxInputChars: 1000,
		Model:         "fake",
		Dimension:     4,
	})
	// RunCell should still persist a row with status='error', returning err==nil
	// (the error is captured in the row, not propagated upward).
	if err != nil {
		t.Fatalf("RunCell should persist row even on backend type-assert failure: %v", err)
	}
	var status, errMsg sql.NullString
	if err := fx.VectorsDB.QueryRow(`SELECT status, error_message FROM bench_runs WHERE id = ?`, runID).Scan(&status, &errMsg); err != nil {
		t.Fatal(err)
	}
	if status.String != "error" {
		t.Errorf("status: got %q, want error", status.String)
	}
	if !errMsg.Valid || !strings.Contains(errMsg.String, "BenchBackend") {
		t.Errorf("error_message should mention BenchBackend; got %q", errMsg.String)
	}
}

func TestRunSweep_Persists_SweepAndRuns(t *testing.T) {
	fx := newOrchFixture(t, 5)
	if err := CreateSampleFromIDs(context.Background(), fx.VectorsDB, "demo", []int64{1, 2, 3, 4, 5}, SampleMeta{}); err != nil {
		t.Fatal(err)
	}
	plan := &Plan{
		Sample: "demo",
		Mode:   "endpoint",
		Matrix: []MatrixAxis{
			{Name: "endpoint", Values: []any{"http://a", "http://b"}},
			{Name: "batch_size", Values: []any{int64(1), int64(2)}},
		},
	}
	cellRunner := func(ctx context.Context, sweepID int64, cell Cell) (RunCellInputs, error) {
		return RunCellInputs{
			SweepID:       &sweepID,
			SampleName:    "demo",
			Mode:          "endpoint",
			Cell:          cell,
			BackendBase:   fx.Backend,
			MainDB:        fx.MainDB,
			VectorsDB:     fx.VectorsDB,
			Client:        fx.Client,
			BatchSize:     int(cell["batch_size"].(int64)),
			Workers:       1,
			MaxInputChars: 1000,
			Model:         "m",
			Dimension:     4,
		}, nil
	}
	sweepID, err := RunSweep(context.Background(), fx.VectorsDB,
		plan, "demo", "raw plan toml", "auto/test.toml", "abc1234", "test sweep", cellRunner)
	if err != nil {
		t.Fatalf("RunSweep: %v", err)
	}
	// Verify bench_sweeps row.
	var status sql.NullString
	var planTOML sql.NullString
	if err := fx.VectorsDB.QueryRow(`SELECT status, plan_toml FROM bench_sweeps WHERE id = ?`, sweepID).Scan(&status, &planTOML); err != nil {
		t.Fatal(err)
	}
	if status.String != "completed" {
		t.Errorf("sweep status: got %q, want completed", status.String)
	}
	if planTOML.String != "raw plan toml" {
		t.Errorf("plan_toml: got %q, want %q", planTOML.String, "raw plan toml")
	}
	// 2 endpoints × 2 batch_sizes = 4 cells = 4 bench_runs rows.
	var n int
	if err := fx.VectorsDB.QueryRow(`SELECT COUNT(*) FROM bench_runs WHERE sweep_id = ?`, sweepID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 4 {
		t.Errorf("bench_runs for sweep: got %d, want 4", n)
	}
}

func TestRunSweep_PreservesPipelineSerialization(t *testing.T) {
	// Two pipeline cells. They MUST run sequentially because the
	// unique partial index on state='building' rejects concurrent
	// bench gens.
	fx := newOrchFixture(t, 3)
	if err := CreateSampleFromIDs(context.Background(), fx.VectorsDB, "demo", []int64{1, 2, 3}, SampleMeta{}); err != nil {
		t.Fatal(err)
	}
	plan := &Plan{
		Sample: "demo",
		Mode:   "pipeline",
		Matrix: []MatrixAxis{
			{Name: "batch_size", Values: []any{int64(1), int64(2)}},
		},
	}
	cellRunner := func(ctx context.Context, sweepID int64, cell Cell) (RunCellInputs, error) {
		return RunCellInputs{
			SweepID:       &sweepID,
			SampleName:    "demo",
			Mode:          "pipeline",
			Cell:          cell,
			BackendBase:   fx.Backend,
			MainDB:        fx.MainDB,
			VectorsDB:     fx.VectorsDB,
			Client:        fx.Client,
			BatchSize:     int(cell["batch_size"].(int64)),
			Workers:       1,
			MaxInputChars: 1000,
			Model:         "fake",
			Dimension:     4,
		}, nil
	}
	sweepID, err := RunSweep(context.Background(), fx.VectorsDB,
		plan, "demo", "p", "", "", "", cellRunner)
	if err != nil {
		t.Fatalf("RunSweep pipeline: %v", err)
	}
	var n int
	if err := fx.VectorsDB.QueryRow(`SELECT COUNT(*) FROM bench_runs WHERE sweep_id = ? AND status = 'completed'`, sweepID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("expected 2 completed pipeline cells, got %d", n)
	}
	// No leftover bench gens.
	var ng int
	if err := fx.VectorsDB.QueryRow(`SELECT COUNT(*) FROM index_generations WHERE fingerprint LIKE 'bench:%'`).Scan(&ng); err != nil {
		t.Fatal(err)
	}
	if ng != 0 {
		t.Errorf("bench gens leaked: %d remaining", ng)
	}
}

// stubBackend (non-BenchBackend) is defined in runner_pipeline_test.go;
// reuse it here.
var _ vector.Backend = stubBackend{}

// Ensure the embed import is used (fakeEmbedClient uses embed.EmbeddingClient
// indirectly via the orchFixture.Client field type).
var _ embed.EmbeddingClient = (*fakeEmbedClient)(nil)
