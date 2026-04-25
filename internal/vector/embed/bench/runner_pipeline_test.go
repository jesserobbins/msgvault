//go:build sqlite_vec

package bench

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"github.com/wesm/msgvault/internal/vector"
	"github.com/wesm/msgvault/internal/vector/embed"
	"github.com/wesm/msgvault/internal/vector/sqlitevec"
)

// pipelineFixture wires a real sqlitevec backend, a main DB with N
// messages, a vectors.db second handle (for queue ops), and a fake
// embed client that always succeeds.
type pipelineFixture struct {
	Backend   *sqlitevec.Backend
	MainDB    *sql.DB
	VectorsDB *sql.DB
	Client    *fakeEmbedClient
	SampleIDs []int64
}

func newPipelineFixture(t *testing.T, n int) *pipelineFixture {
	t.Helper()
	if err := sqlitevec.RegisterExtension(); err != nil {
		t.Fatalf("RegisterExtension: %v", err)
	}
	dir := t.TempDir()
	mainPath := filepath.Join(dir, "main.db")
	mainDB, err := sql.Open(sqlitevec.DriverName(), mainPath)
	if err != nil {
		t.Fatalf("open main: %v", err)
	}
	t.Cleanup(func() { _ = mainDB.Close() })
	if _, err := mainDB.Exec(`
CREATE TABLE messages (id INTEGER PRIMARY KEY, subject TEXT, deleted_from_source_at DATETIME);
CREATE TABLE message_bodies (message_id INTEGER PRIMARY KEY, body_text TEXT, body_html TEXT);`); err != nil {
		t.Fatalf("schema: %v", err)
	}
	var ids []int64
	for i := 1; i <= n; i++ {
		if _, err := mainDB.Exec(`INSERT INTO messages (id, subject) VALUES (?, ?)`, i, "subj"); err != nil {
			t.Fatalf("insert msg: %v", err)
		}
		if _, err := mainDB.Exec(`INSERT INTO message_bodies (message_id, body_text) VALUES (?, ?)`, i, "body text"); err != nil {
			t.Fatalf("insert body: %v", err)
		}
		ids = append(ids, int64(i))
	}
	vecPath := filepath.Join(dir, "vectors.db")
	b, err := sqlitevec.Open(context.Background(), sqlitevec.Options{
		Path:      vecPath,
		MainPath:  mainPath,
		Dimension: 4,
		MainDB:    mainDB,
	})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	vecDB, err := sql.Open(sqlitevec.DriverName(), vecPath)
	if err != nil {
		t.Fatalf("open vec handle: %v", err)
	}
	t.Cleanup(func() { _ = vecDB.Close() })
	return &pipelineFixture{
		Backend:   b,
		MainDB:    mainDB,
		VectorsDB: vecDB,
		Client:    &fakeEmbedClient{dim: 4},
		SampleIDs: ids,
	}
}

// stubBackend implements vector.Backend (NOT BenchBackend) for the
// type-assertion-failure test.
type stubBackend struct{}

func (stubBackend) CreateGeneration(ctx context.Context, model string, dim int) (vector.GenerationID, error) {
	return 0, nil
}
func (stubBackend) ActivateGeneration(ctx context.Context, gen vector.GenerationID) error { return nil }
func (stubBackend) RetireGeneration(ctx context.Context, gen vector.GenerationID) error   { return nil }
func (stubBackend) ActiveGeneration(ctx context.Context) (vector.Generation, error) {
	return vector.Generation{}, nil
}
func (stubBackend) BuildingGeneration(ctx context.Context) (*vector.Generation, error) {
	return nil, nil
}
func (stubBackend) Upsert(ctx context.Context, gen vector.GenerationID, chunks []vector.Chunk) error {
	return nil
}
func (stubBackend) Search(ctx context.Context, gen vector.GenerationID, queryVec []float32, k int, filter vector.Filter) ([]vector.Hit, error) {
	return nil, nil
}
func (stubBackend) Delete(ctx context.Context, gen vector.GenerationID, messageIDs []int64) error {
	return nil
}
func (stubBackend) Stats(ctx context.Context, gen vector.GenerationID) (vector.Stats, error) {
	return vector.Stats{}, nil
}
func (stubBackend) EnsureSeeded(ctx context.Context, gen vector.GenerationID) error { return nil }
func (stubBackend) LoadVector(ctx context.Context, messageID int64) ([]float32, error) {
	return nil, nil
}
func (stubBackend) Close() error { return nil }

func TestRunPipeline_RejectsNonBenchBackend(t *testing.T) {
	_, err := RunPipeline(context.Background(), PipelineInputs{
		BackendBase: stubBackend{},
		Model:       "x",
		Dimension:   4,
		RunScope:    "1:1",
	})
	if err == nil {
		t.Fatal("expected error for non-BenchBackend")
	}
	if !strings.Contains(err.Error(), "BenchBackend") {
		t.Errorf("error should mention BenchBackend; got %v", err)
	}
}

func TestRunPipeline_HappyPath(t *testing.T) {
	fx := newPipelineFixture(t, 3)
	res, err := RunPipeline(context.Background(), PipelineInputs{
		SampleIDs:     fx.SampleIDs,
		BackendBase:   fx.Backend,
		MainDB:        fx.MainDB,
		VectorsDB:     fx.VectorsDB,
		Client:        fx.Client,
		BatchSize:     2,
		Workers:       1,
		Model:         "fake",
		Dimension:     4,
		RunScope:      "1:1",
		MaxInputChars: 1000,
	})
	if err != nil {
		t.Fatalf("RunPipeline: %v", err)
	}
	if res == nil {
		t.Fatal("nil result")
	}
	// Generation must have been dropped on return.
	var n int
	if err := fx.VectorsDB.QueryRow(`SELECT COUNT(*) FROM index_generations WHERE id = ?`, int64(res.GenerationID)).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("throwaway gen not dropped: %d rows remain", n)
	}
	// Aggregator should have recorded at least the embedded messages.
	agg := res.Aggregator.Result()
	if agg.Msgs < 1 {
		t.Errorf("Msgs: got %d, want >=1", agg.Msgs)
	}
}

func TestRunPipeline_DropsGenOnContextCancel(t *testing.T) {
	fx := newPipelineFixture(t, 5)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel: workers exit immediately

	res, err := RunPipeline(ctx, PipelineInputs{
		SampleIDs:     fx.SampleIDs,
		BackendBase:   fx.Backend,
		MainDB:        fx.MainDB,
		VectorsDB:     fx.VectorsDB,
		Client:        fx.Client,
		BatchSize:     2,
		Workers:       1,
		Model:         "fake",
		Dimension:     4,
		RunScope:      "cancel:1",
		MaxInputChars: 1000,
	})
	// Either context.Canceled or wrapped — both acceptable.
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Logf("RunPipeline err: %v (acceptable)", err)
	}
	// Even on cancel, the gen must be dropped.
	if res != nil && res.GenerationID != 0 {
		var n int
		if err := fx.VectorsDB.QueryRow(`SELECT COUNT(*) FROM index_generations WHERE id = ?`, int64(res.GenerationID)).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Errorf("gen not dropped on cancel: %d rows remain", n)
		}
	}
}

func TestRunPipeline_NoSeedFromCorpus(t *testing.T) {
	// Create a fixture with 5 messages in main, but pass only IDs 1
	// and 2 as the SampleIDs. The runner must seed pending_embeddings
	// with ONLY those two — not the full corpus. We verify by setting
	// BatchSize=1, Workers=1, then checking after RunPipeline returns
	// that exactly 2 messages were processed (Msgs == 2).
	fx := newPipelineFixture(t, 5)
	res, err := RunPipeline(context.Background(), PipelineInputs{
		SampleIDs:     []int64{1, 2}, // subset
		BackendBase:   fx.Backend,
		MainDB:        fx.MainDB,
		VectorsDB:     fx.VectorsDB,
		Client:        fx.Client,
		BatchSize:     1,
		Workers:       1,
		Model:         "fake",
		Dimension:     4,
		RunScope:      "subset:1",
		MaxInputChars: 1000,
	})
	if err != nil {
		t.Fatalf("RunPipeline: %v", err)
	}
	agg := res.Aggregator.Result()
	if agg.Msgs != 2 {
		t.Errorf("Msgs: got %d, want 2 (only sampled IDs should be processed)", agg.Msgs)
	}
}

// TestRunPipeline_EmbedDurClampedAtZero relies on the recordPipelineBatch
// helper to clamp negative residuals at zero. This guards against
// accidentally propagating negatives if BatchElapsed < sum of phases.
func TestRunPipeline_EmbedDurClampedAtZero(t *testing.T) {
	// We can't easily produce a real negative from a worker, but we
	// can call recordPipelineBatch directly with a synthetic report
	// whose phase sums exceed BatchElapsed.
	a := NewAggregator()
	a.Start()
	recordPipelineBatch(a, embed.ProgressReport{
		BatchMsgs:       5,
		BatchChars:      100,
		BatchElapsed:    10 * time.Millisecond,
		ClaimElapsed:    20 * time.Millisecond, // exceeds BatchElapsed
		UpsertElapsed:   5 * time.Millisecond,
		CompleteElapsed: 5 * time.Millisecond,
	})
	a.Stop()
	r := a.Result()
	// Embed time should clamp at 0, not go negative. The
	// EmbedMsSum should equal 0 (the clamp) for this single batch.
	if r.EmbedMsSum < 0 {
		t.Errorf("EmbedMsSum negative: %d", r.EmbedMsSum)
	}
}
