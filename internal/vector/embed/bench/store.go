// Package bench provides the embedshootout benchmark harness library:
// stratified sample sets, plan/matrix expansion, endpoint and pipeline
// mode runners, results storage, and reports. All bench tables are
// created lazily on first write so a fresh vectors.db stays clean until
// a benchmark or sample-create is explicitly invoked.
package bench

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// ErrNoBenchData is returned by RequireSchema when the bench tables have not
// been created yet (i.e. no benchmark or sample-create has ever been run).
var ErrNoBenchData = errors.New("bench: no bench data in this vectors.db (run sample-create or run a benchmark first)")

const benchSchemaSQL = `
CREATE TABLE IF NOT EXISTS bench_samples (
    name           TEXT PRIMARY KEY,
    created_at     INTEGER NOT NULL,
    size           INTEGER NOT NULL,
    stratify_spec  TEXT,
    seed           INTEGER,
    notes          TEXT
);
CREATE TABLE IF NOT EXISTS bench_sample_messages (
    sample_name    TEXT NOT NULL REFERENCES bench_samples(name) ON DELETE CASCADE,
    message_id     INTEGER NOT NULL,
    stratum        TEXT,
    PRIMARY KEY (sample_name, message_id)
);
CREATE TABLE IF NOT EXISTS bench_sweeps (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at     INTEGER NOT NULL,
    finished_at    INTEGER,
    status         TEXT NOT NULL,
    plan_path      TEXT,
    plan_toml      TEXT NOT NULL,
    git_sha        TEXT,
    notes          TEXT
);
CREATE TABLE IF NOT EXISTS bench_runs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    sweep_id        INTEGER REFERENCES bench_sweeps(id) ON DELETE CASCADE,
    started_at      INTEGER NOT NULL,
    finished_at     INTEGER,
    status          TEXT NOT NULL,
    error_message   TEXT,
    mode            TEXT NOT NULL,
    sample_name     TEXT NOT NULL,
    config_json     TEXT NOT NULL,
    config_hash     TEXT NOT NULL,
    cell_json       TEXT NOT NULL,
    -- generation_id is a historical/audit reference, NOT a foreign
    -- key. RunPipeline drops the throwaway generation before
    -- returning, so by the time bench_runs is committed the
    -- referenced index_generations row has already been deleted.
    -- Stored solely so post-hoc orphan investigations can correlate
    -- "this run touched gen 12345" with logs.
    generation_id   INTEGER,
    msgs_total      INTEGER,
    msgs_succeeded  INTEGER,
    msgs_truncated  INTEGER,
    msgs_dropped    INTEGER,
    chars_total     INTEGER,
    elapsed_ms      INTEGER,
    embed_ms_sum    INTEGER,
    msg_per_sec     REAL,
    us_per_char     REAL,
    ms_per_msg      REAL,
    batch_p50_ms    REAL,
    batch_p95_ms    REAL,
    batch_p99_ms    REAL,
    batch_max_ms    REAL,
    errors_4xx      INTEGER,
    errors_5xx      INTEGER,
    errors_429      INTEGER,
    errors_network  INTEGER,
    retries         INTEGER,
    claim_ms_sum    INTEGER,
    upsert_ms_sum   INTEGER,
    complete_ms_sum INTEGER
);
CREATE INDEX IF NOT EXISTS bench_runs_sweep_idx       ON bench_runs(sweep_id);
CREATE INDEX IF NOT EXISTS bench_runs_config_hash_idx ON bench_runs(config_hash);
`

// EnsureSchema creates the bench tables and indexes if they do not already
// exist. It is safe to call on a database that already has the schema applied
// (idempotent via CREATE TABLE IF NOT EXISTS). Foreign-key cascades in this
// schema require PRAGMA foreign_keys = ON; the standard vectors.db open path
// enables this via sqlitevec.RegisterExtension's ConnectHook.
func EnsureSchema(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, benchSchemaSQL); err != nil {
		return fmt.Errorf("bench: ensure schema: %w", err)
	}
	return nil
}

// RequireSchema returns ErrNoBenchData if the bench tables have not been
// created, and nil if they are present. It does not create any tables.
func RequireSchema(ctx context.Context, db *sql.DB) error {
	var name string
	err := db.QueryRowContext(ctx,
		`SELECT name FROM sqlite_master WHERE type='table' AND name='bench_samples'`).Scan(&name)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNoBenchData
	}
	return err
}
