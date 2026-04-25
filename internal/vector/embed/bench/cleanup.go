//go:build sqlite_vec

package bench

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/wesm/msgvault/internal/vector"
)

// CleanupOrphanGenerations finds bench: generations whose started_at
// is older than olderThan and which are NOT referenced by any
// bench_runs row with status='running', then drops each via
// BenchBackend.DropGeneration. Returns the count of dropped
// generations.
//
// This is the "did a previous run crash mid-pipeline?" recovery
// hatch. RunPipeline drops its throwaway gen on every return path
// (including ctx-cancel) via a defer, so the only way an orphan
// reaches the cleanup query is a process kill (SIGKILL, OOM, etc.)
// before the defer runs.
func CleanupOrphanGenerations(ctx context.Context, db *sql.DB, bb vector.BenchBackend, olderThan time.Duration) (int, error) {
	cutoff := time.Now().Add(-olderThan).Unix()
	rows, err := db.QueryContext(ctx, `
        SELECT g.id
          FROM index_generations g
         WHERE g.fingerprint LIKE 'bench:%'
           AND g.started_at < ?
           AND NOT EXISTS (
               SELECT 1 FROM bench_runs r
                WHERE r.generation_id = g.id
                  AND r.status = 'running')`, cutoff)
	if err != nil {
		return 0, fmt.Errorf("cleanup query: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var ids []vector.GenerationID
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return 0, err
		}
		ids = append(ids, vector.GenerationID(id))
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	n := 0
	for _, id := range ids {
		if err := bb.DropGeneration(ctx, id); err != nil {
			return n, fmt.Errorf("drop %d: %w", id, err)
		}
		n++
	}
	return n, nil
}
