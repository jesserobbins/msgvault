//go:build sqlite_vec

package sqlitevec

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/wesm/msgvault/internal/vector"
)

const benchFingerprintPrefix = "bench:"

// CreateBenchGeneration implements vector.BenchBackend. See the
// interface doc in internal/vector/backend.go for the contract.
func (b *Backend) CreateBenchGeneration(ctx context.Context, model string, dim int, runScope string) (vector.GenerationID, error) {
	if strings.TrimSpace(runScope) == "" {
		return 0, fmt.Errorf("CreateBenchGeneration: runScope must be non-empty")
	}
	if err := EnsureVectorTable(ctx, b.db, dim); err != nil {
		return 0, err
	}
	fp := fmt.Sprintf("%s%s:%s:%d", benchFingerprintPrefix, runScope, model, dim)
	now := time.Now().Unix()

	// index_generations has a unique partial index on state='building'
	// — only one building generation can exist at a time. A bench
	// generation cannot share that slot with a production rebuild.
	// Detect and surface that case with an actionable error rather
	// than letting the unique-constraint failure bubble up raw.
	if existingFP, ok, err := b.lookupBuildingFingerprint(ctx); err != nil {
		return 0, err
	} else if ok {
		return 0, fmt.Errorf("CreateBenchGeneration: a building generation (%q) already exists; pipeline-mode bench cannot run during a production rebuild", existingFP)
	}

	res, err := b.db.ExecContext(ctx,
		`INSERT INTO index_generations
		 (model, dimension, fingerprint, started_at, seeded_at, state)
		 VALUES (?, ?, ?, ?, ?, 'building')`,
		model, dim, fp, now, now)
	if err != nil {
		return 0, fmt.Errorf("CreateBenchGeneration: insert: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("CreateBenchGeneration: last insert id: %w", err)
	}
	return vector.GenerationID(id), nil
}

// lookupBuildingFingerprint returns the fingerprint of the current
// building gen, or ok=false if none exists.
func (b *Backend) lookupBuildingFingerprint(ctx context.Context) (string, bool, error) {
	var fp string
	err := b.db.QueryRowContext(ctx,
		`SELECT fingerprint FROM index_generations WHERE state = 'building'`).Scan(&fp)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("lookup building: %w", err)
	}
	return fp, true, nil
}

// DropGeneration implements vector.BenchBackend.
func (b *Backend) DropGeneration(ctx context.Context, gen vector.GenerationID) error {
	var fp string
	var dim int
	err := b.db.QueryRowContext(ctx,
		`SELECT fingerprint, dimension FROM index_generations WHERE id = ?`, int64(gen)).Scan(&fp, &dim)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("DropGeneration: unknown generation %d", gen)
	}
	if err != nil {
		return fmt.Errorf("DropGeneration: lookup: %w", err)
	}
	if !strings.HasPrefix(fp, benchFingerprintPrefix) {
		return fmt.Errorf("DropGeneration: refusing to drop non-bench generation %d (fingerprint=%q)", gen, fp)
	}

	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("DropGeneration: begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	vecTable := VectorTableName(dim)
	if _, err := tx.ExecContext(ctx, `DELETE FROM embeddings WHERE generation_id = ?`, int64(gen)); err != nil {
		return fmt.Errorf("DropGeneration: delete embeddings: %w", err)
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s WHERE generation_id = ?`, vecTable), int64(gen)); err != nil {
		return fmt.Errorf("DropGeneration: delete %s: %w", vecTable, err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM pending_embeddings WHERE generation_id = ?`, int64(gen)); err != nil {
		return fmt.Errorf("DropGeneration: delete pending: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM index_generations WHERE id = ?`, int64(gen)); err != nil {
		return fmt.Errorf("DropGeneration: delete generation row: %w", err)
	}
	return tx.Commit()
}
