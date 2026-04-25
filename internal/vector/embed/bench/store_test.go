package bench

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func openMem(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestEnsureSchema_CreatesTables(t *testing.T) {
	db := openMem(t)
	if err := EnsureSchema(context.Background(), db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	for _, tbl := range []string{
		"bench_samples", "bench_sample_messages",
		"bench_sweeps", "bench_runs",
	} {
		var name string
		err := db.QueryRow(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`, tbl).Scan(&name)
		if err != nil {
			t.Errorf("table %s not created: %v", tbl, err)
		}
	}
}

func TestEnsureSchema_Idempotent(t *testing.T) {
	db := openMem(t)
	ctx := context.Background()
	if err := EnsureSchema(ctx, db); err != nil {
		t.Fatal(err)
	}
	if err := EnsureSchema(ctx, db); err != nil {
		t.Fatalf("second EnsureSchema: %v", err)
	}
}

func TestRequireSchema_ErrWhenAbsent(t *testing.T) {
	db := openMem(t)
	err := RequireSchema(context.Background(), db)
	if !errors.Is(err, ErrNoBenchData) {
		t.Fatalf("RequireSchema: got %v, want ErrNoBenchData", err)
	}
}
