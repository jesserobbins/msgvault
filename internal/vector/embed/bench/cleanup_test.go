//go:build sqlite_vec

package bench

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"github.com/wesm/msgvault/internal/vector/sqlitevec"
)

// openCleanupBackend opens a real sqlitevec.Backend with the bench
// schema lazy-initialized and FK enforcement on (per the ConnectHook).
func openCleanupBackend(t *testing.T) (*sqlitevec.Backend, string) {
	t.Helper()
	if err := sqlitevec.RegisterExtension(); err != nil {
		t.Fatalf("RegisterExtension: %v", err)
	}
	dir := t.TempDir()
	vecPath := filepath.Join(dir, "vectors.db")
	b, err := sqlitevec.Open(context.Background(), sqlitevec.Options{
		Path:      vecPath,
		Dimension: 4,
	})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b, vecPath
}

func TestCleanup_DropsOldOrphan(t *testing.T) {
	b, _ := openCleanupBackend(t)
	ctx := context.Background()
	if err := EnsureSchema(ctx, b.DB()); err != nil {
		t.Fatal(err)
	}
	gen, err := b.CreateBenchGeneration(ctx, "x", 4, "scope1")
	if err != nil {
		t.Fatal(err)
	}
	// Backdate started_at to be older than the threshold.
	if _, err := b.DB().Exec(`UPDATE index_generations SET started_at = ? WHERE id = ?`,
		time.Now().Add(-2*time.Hour).Unix(), int64(gen)); err != nil {
		t.Fatal(err)
	}
	n, err := CleanupOrphanGenerations(ctx, b.DB(), b, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("dropped count: got %d, want 1", n)
	}
	// Gen should be gone.
	var exists int
	_ = b.DB().QueryRow(`SELECT COUNT(*) FROM index_generations WHERE id = ?`, int64(gen)).Scan(&exists)
	if exists != 0 {
		t.Errorf("gen not dropped: %d rows", exists)
	}
}

func TestCleanup_PreservesYoungOrphan(t *testing.T) {
	b, _ := openCleanupBackend(t)
	ctx := context.Background()
	if err := EnsureSchema(ctx, b.DB()); err != nil {
		t.Fatal(err)
	}
	gen, err := b.CreateBenchGeneration(ctx, "x", 4, "scope1")
	if err != nil {
		t.Fatal(err)
	}
	// started_at is now (just created); threshold of 1 hour means
	// this gen is too young to drop.
	n, err := CleanupOrphanGenerations(ctx, b.DB(), b, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("dropped count: got %d, want 0 (gen is young)", n)
	}
	var exists int
	_ = b.DB().QueryRow(`SELECT COUNT(*) FROM index_generations WHERE id = ?`, int64(gen)).Scan(&exists)
	if exists != 1 {
		t.Errorf("young gen wrongly dropped")
	}
}

func TestCleanup_PreservesGenWithRunningRun(t *testing.T) {
	b, _ := openCleanupBackend(t)
	ctx := context.Background()
	if err := EnsureSchema(ctx, b.DB()); err != nil {
		t.Fatal(err)
	}
	gen, err := b.CreateBenchGeneration(ctx, "x", 4, "scope1")
	if err != nil {
		t.Fatal(err)
	}
	// Backdate so the age check would fire.
	if _, err := b.DB().Exec(`UPDATE index_generations SET started_at = ? WHERE id = ?`,
		time.Now().Add(-2*time.Hour).Unix(), int64(gen)); err != nil {
		t.Fatal(err)
	}
	// Insert a bench_runs row referencing this gen with status='running'.
	if _, err := b.DB().Exec(`
        INSERT INTO bench_runs (started_at, status, mode, sample_name, config_json, config_hash, cell_json, generation_id)
        VALUES (?, 'running', 'pipeline', 'sample-x', '{}', 'h', '{}', ?)`,
		time.Now().Unix(), int64(gen)); err != nil {
		t.Fatal(err)
	}
	n, err := CleanupOrphanGenerations(ctx, b.DB(), b, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("dropped count: got %d, want 0 (gen has running run)", n)
	}
	var exists int
	_ = b.DB().QueryRow(`SELECT COUNT(*) FROM index_generations WHERE id = ?`, int64(gen)).Scan(&exists)
	if exists != 1 {
		t.Errorf("gen with running run wrongly dropped")
	}
}

func TestCleanup_IgnoresNonBenchGen(t *testing.T) {
	b, _ := openCleanupBackend(t)
	ctx := context.Background()
	if err := EnsureSchema(ctx, b.DB()); err != nil {
		t.Fatal(err)
	}
	// Create a non-bench generation directly.
	res, err := b.DB().Exec(`
        INSERT INTO index_generations (model, dimension, fingerprint, started_at, state)
        VALUES ('real', 4, 'real:4', ?, 'building')`,
		time.Now().Add(-2*time.Hour).Unix())
	if err != nil {
		t.Fatal(err)
	}
	id, _ := res.LastInsertId()

	n, err := CleanupOrphanGenerations(ctx, b.DB(), b, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("dropped count: got %d, want 0 (non-bench gen)", n)
	}
	var exists int
	_ = b.DB().QueryRow(`SELECT COUNT(*) FROM index_generations WHERE id = ?`, id).Scan(&exists)
	if exists != 1 {
		t.Errorf("non-bench gen wrongly dropped")
	}
}

func TestCleanup_EmptyDB(t *testing.T) {
	b, _ := openCleanupBackend(t)
	ctx := context.Background()
	if err := EnsureSchema(ctx, b.DB()); err != nil {
		t.Fatal(err)
	}
	n, err := CleanupOrphanGenerations(ctx, b.DB(), b, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("got %d, want 0", n)
	}
}
