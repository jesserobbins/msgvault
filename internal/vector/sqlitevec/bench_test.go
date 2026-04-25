//go:build sqlite_vec

package sqlitevec

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wesm/msgvault/internal/vector"
)

// Compile-time assertion that *Backend satisfies vector.BenchBackend.
var _ vector.BenchBackend = (*Backend)(nil)

// openBenchTestBackend opens a fresh sqlitevec.Backend backed by a
// minimal in-memory main DB with one non-deleted message (id=1).
func openBenchTestBackend(t *testing.T) (*Backend, *sql.DB) {
	t.Helper()
	main := openMainDBWithOneMessage(t)
	ctx := context.Background()
	b, err := Open(ctx, Options{
		Path:      filepath.Join(t.TempDir(), "vectors.db"),
		Dimension: 8,
		MainDB:    main,
	})
	if err != nil {
		t.Fatalf("Open backend: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b, main
}

func TestCreateBenchGeneration_ForcesBenchPrefix(t *testing.T) {
	b, _ := openBenchTestBackend(t)
	gen, err := b.CreateBenchGeneration(context.Background(), "nomic-embed-text", 768, "7:42")
	if err != nil {
		t.Fatalf("CreateBenchGeneration: %v", err)
	}
	var fp string
	if err := b.db.QueryRow(`SELECT fingerprint FROM index_generations WHERE id = ?`, int64(gen)).Scan(&fp); err != nil {
		t.Fatal(err)
	}
	want := "bench:7:42:nomic-embed-text:768"
	if fp != want {
		t.Errorf("fingerprint: got %q, want %q", fp, want)
	}
}

func TestCreateBenchGeneration_NoSeedPass(t *testing.T) {
	b, _ := openBenchTestBackend(t)
	gen, err := b.CreateBenchGeneration(context.Background(), "x", 8, "scope")
	if err != nil {
		t.Fatal(err)
	}
	var n int
	if err := b.db.QueryRow(`SELECT COUNT(*) FROM pending_embeddings WHERE generation_id = ?`, int64(gen)).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("bench gen seeded pending: got %d rows, want 0", n)
	}
}

func TestCreateBenchGeneration_EnsureSeededIsNoOp(t *testing.T) {
	b, _ := openBenchTestBackend(t)
	gen, err := b.CreateBenchGeneration(context.Background(), "x", 8, "scope")
	if err != nil {
		t.Fatal(err)
	}
	if err := b.EnsureSeeded(context.Background(), gen); err != nil {
		t.Fatalf("EnsureSeeded against bench gen: %v", err)
	}
	var n int
	if err := b.db.QueryRow(`SELECT COUNT(*) FROM pending_embeddings WHERE generation_id = ?`, int64(gen)).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("EnsureSeeded leaked pending: got %d rows, want 0", n)
	}
}

func TestCreateBenchGeneration_RejectsEmptyScope(t *testing.T) {
	b, _ := openBenchTestBackend(t)
	_, err := b.CreateBenchGeneration(context.Background(), "x", 8, "")
	if err == nil {
		t.Fatal("expected error for empty runScope")
	}
	if !strings.Contains(err.Error(), "runScope") {
		t.Errorf("error should mention runScope; got %v", err)
	}
}

func TestCreateBenchGeneration_RejectsWhitespaceScope(t *testing.T) {
	b, _ := openBenchTestBackend(t)
	_, err := b.CreateBenchGeneration(context.Background(), "x", 8, "   ")
	if err == nil {
		t.Fatal("expected error for whitespace-only runScope")
	}
	if !strings.Contains(err.Error(), "runScope") {
		t.Errorf("error should mention runScope; got %v", err)
	}
}

func TestCreateBenchGeneration_RejectsWhenProductionBuildingExists(t *testing.T) {
	b, _ := openBenchTestBackend(t)
	// Create a production building gen first.
	if _, err := b.CreateGeneration(context.Background(), "real-model", 8); err != nil {
		t.Fatal(err)
	}
	_, err := b.CreateBenchGeneration(context.Background(), "x", 8, "scope")
	if err == nil {
		t.Fatal("expected error: bench gen requested while production rebuild active")
	}
}

func TestDropGeneration_BenchOnly(t *testing.T) {
	b, _ := openBenchTestBackend(t)
	gen, err := b.CreateBenchGeneration(context.Background(), "x", 8, "scope")
	if err != nil {
		t.Fatal(err)
	}
	if err := b.DropGeneration(context.Background(), gen); err != nil {
		t.Fatalf("DropGeneration bench: %v", err)
	}
	var exists int
	if err := b.db.QueryRow(`SELECT COUNT(*) FROM index_generations WHERE id = ?`, int64(gen)).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists != 0 {
		t.Errorf("bench gen not deleted: %d rows", exists)
	}
}

func TestDropGeneration_RefusesNonBench(t *testing.T) {
	b, _ := openBenchTestBackend(t)
	gen, err := b.CreateGeneration(context.Background(), "real-model", 8)
	if err != nil {
		t.Fatal(err)
	}
	err = b.DropGeneration(context.Background(), gen)
	if err == nil {
		t.Fatal("DropGeneration accepted non-bench fingerprint")
	}
	if !strings.Contains(err.Error(), "non-bench") {
		t.Errorf("error should mention non-bench; got %v", err)
	}
	var exists int
	if err := b.db.QueryRow(`SELECT COUNT(*) FROM index_generations WHERE id = ?`, int64(gen)).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists != 1 {
		t.Errorf("production gen wrongly deleted")
	}
}

func TestDropGeneration_UnknownGen(t *testing.T) {
	b, _ := openBenchTestBackend(t)
	err := b.DropGeneration(context.Background(), 99999)
	if err == nil {
		t.Fatal("expected error for unknown gen")
	}
}
