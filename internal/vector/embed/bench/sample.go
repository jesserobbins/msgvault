package bench

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/wesm/msgvault/internal/mime"
	"github.com/wesm/msgvault/internal/vector/embed"
)

// SampleMeta carries the metadata stored on bench_samples: seed,
// stratify spec (canonical-JSON-encoded; empty when unstratified),
// and freeform notes.
type SampleMeta struct {
	Seed         int64
	StratifySpec string
	Notes        string
}

// SampleSummary is the row shape returned by ListSamples and
// sample-show.
type SampleSummary struct {
	Name         string
	CreatedAt    int64
	Size         int
	StratifySpec string
	Seed         int64
	Notes        string
}

// CreateSampleFromIDs persists a sample with the given message IDs and
// optional per-ID stratum tags. Errors if the sample name already
// exists. When provided, stratum tags must be the same length as ids.
//
// strata is variadic to keep the unstratified call site clean. Pass
// at most one slice; passing more than one is a programming error.
func CreateSampleFromIDs(ctx context.Context, db *sql.DB, name string, ids []int64, meta SampleMeta, strata ...[]string) error {
	if name == "" {
		return fmt.Errorf("CreateSampleFromIDs: name must be non-empty")
	}
	if err := EnsureSchema(ctx, db); err != nil {
		return err
	}
	var stratum []string
	if len(strata) == 1 {
		stratum = strata[0]
		if len(stratum) != len(ids) {
			return fmt.Errorf("CreateSampleFromIDs: stratum length %d != ids length %d", len(stratum), len(ids))
		}
	} else if len(strata) > 1 {
		return fmt.Errorf("CreateSampleFromIDs: at most one stratum slice")
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	var stratifyJSON sql.NullString
	if meta.StratifySpec != "" {
		stratifyJSON = sql.NullString{String: meta.StratifySpec, Valid: true}
	}
	var seedNull sql.NullInt64
	if meta.Seed != 0 {
		seedNull = sql.NullInt64{Int64: meta.Seed, Valid: true}
	}
	if _, err := tx.ExecContext(ctx, `
        INSERT INTO bench_samples (name, created_at, size, stratify_spec, seed, notes)
        VALUES (?, ?, ?, ?, ?, ?)`,
		name, time.Now().Unix(), len(ids), stratifyJSON, seedNull, meta.Notes); err != nil {
		return fmt.Errorf("insert bench_samples: %w", err)
	}
	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO bench_sample_messages (sample_name, message_id, stratum) VALUES (?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("prepare bench_sample_messages: %w", err)
	}
	defer func() { _ = stmt.Close() }()
	for i, id := range ids {
		var s sql.NullString
		if stratum != nil && stratum[i] != "" {
			s = sql.NullString{String: stratum[i], Valid: true}
		}
		if _, err := stmt.ExecContext(ctx, name, id, s); err != nil {
			return fmt.Errorf("insert sample message %d: %w", id, err)
		}
	}
	return tx.Commit()
}

// SampleMessageIDs returns the message IDs in the sample, sorted by
// message_id ascending (the table's PK order).
func SampleMessageIDs(ctx context.Context, db *sql.DB, name string) ([]int64, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT message_id FROM bench_sample_messages WHERE sample_name = ? ORDER BY message_id`, name)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// SampleExists reports whether a sample with this name has been
// recorded.
func SampleExists(ctx context.Context, db *sql.DB, name string) (bool, error) {
	var n int
	err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM bench_samples WHERE name = ?`, name).Scan(&n)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return n > 0, err
}

// DeleteSample removes a sample (cascade-deletes its message rows).
// PRAGMA foreign_keys must be ON for the cascade to fire — vectors.db
// has it enabled via the sqlitevec ConnectHook; tests must enable it
// explicitly via the openMem helper in store_test.go.
func DeleteSample(ctx context.Context, db *sql.DB, name string) error {
	_, err := db.ExecContext(ctx, `DELETE FROM bench_samples WHERE name = ?`, name)
	return err
}

// ListSamples returns all samples ordered by created_at descending
// (newest first). Errors with ErrNoBenchData if the bench tables
// haven't been created.
func ListSamples(ctx context.Context, db *sql.DB) ([]SampleSummary, error) {
	if err := RequireSchema(ctx, db); err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx,
		`SELECT name, created_at, size, COALESCE(stratify_spec,''), COALESCE(seed,0), COALESCE(notes,'')
           FROM bench_samples ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []SampleSummary
	for rows.Next() {
		var s SampleSummary
		if err := rows.Scan(&s.Name, &s.CreatedAt, &s.Size, &s.StratifySpec, &s.Seed, &s.Notes); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// StratifySpec describes the stratification request: which axes to
// stratify on and the per-bucket weights. All fields are optional —
// an axis with zero entries is treated as absent.
type StratifySpec struct {
	Length      map[string]float64 `json:"length,omitempty"`      // "short" / "medium" / "long" (post-preprocess char buckets)
	Year        map[string]float64 `json:"year,omitempty"`        // "2014" → weight
	Account     map[string]float64 `json:"account,omitempty"`     // sources.identifier → weight
	Attachments map[string]float64 `json:"attachments,omitempty"` // "with" / "without"
	SourceType  map[string]float64 `json:"source_type,omitempty"` // sources.kind → weight
}

// ParseStratifySpec parses the CLI -stratify string into a
// StratifySpec. Format: `axis=v1[:wpct],v2[:wpct],...`.
// Per-axis weights must sum to 1.0 (within ±0.01); equal weighting
// is implied when no :pct is given. Only one axis may be specified;
// multi-axis stratification is not implemented.
func ParseStratifySpec(s string) (StratifySpec, error) {
	var spec StratifySpec
	if s == "" {
		return spec, nil
	}
	axes := 0
	for group := range strings.SplitSeq(s, ";") {
		axis, list, ok := strings.Cut(group, "=")
		if !ok {
			return spec, fmt.Errorf("stratify: %q: missing '='", group)
		}
		axis = strings.TrimSpace(axis)
		weights, err := parseWeightList(list)
		if err != nil {
			return spec, fmt.Errorf("stratify axis %q: %w", axis, err)
		}
		switch axis {
		case "length":
			spec.Length = weights
		case "year":
			spec.Year = weights
		case "account":
			spec.Account = weights
		case "attachments":
			spec.Attachments = weights
		case "source_type":
			spec.SourceType = weights
		default:
			return spec, fmt.Errorf("stratify: unknown axis %q (valid: length, year, account, attachments, source_type)", axis)
		}
		axes++
	}
	if axes > 1 {
		return StratifySpec{}, fmt.Errorf("stratify: multi-axis stratification is not supported; specify one axis")
	}
	return spec, nil
}

func parseWeightList(s string) (map[string]float64, error) {
	parts := strings.Split(s, ",")
	weights := make(map[string]float64, len(parts))
	var withPct, withoutPct []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		key, wStr, ok := strings.Cut(p, ":")
		if !ok {
			withoutPct = append(withoutPct, p)
			continue
		}
		key = strings.TrimSpace(key)
		wStr = strings.TrimSpace(wStr)
		wStr = strings.TrimSuffix(wStr, "%")
		pct, err := strconv.ParseFloat(wStr, 64)
		if err != nil {
			return nil, fmt.Errorf("weight %q: %w", p, err)
		}
		weights[key] = pct / 100.0
		withPct = append(withPct, key)
	}
	if len(withPct) > 0 && len(withoutPct) > 0 {
		return nil, fmt.Errorf("mix of explicit and implicit weights not allowed; use one consistent style")
	}
	if len(withoutPct) > 0 {
		share := 1.0 / float64(len(withoutPct))
		for _, k := range withoutPct {
			weights[k] = share
		}
	}
	var total float64
	for _, w := range weights {
		total += w
	}
	if total < 0.99 || total > 1.01 {
		return nil, fmt.Errorf("weights sum to %.2f, expected 1.0", total)
	}
	return weights, nil
}

// CreateStratifiedSample selects size message IDs from mainDB,
// bucketed per spec, and persists them as a sample in vecDB. With
// an empty StratifySpec it falls back to uniform random sampling.
//
// Reads subjects + bodies from main DB (read-only) and runs
// embed.Preprocess so the length bucketing reflects what the
// embedder will actually see.
func CreateStratifiedSample(ctx context.Context, vecDB, mainDB *sql.DB, name string, size int, spec StratifySpec, seed int64, pp embed.PreprocessConfig, maxInputChars int, notes string) error {
	rows, err := mainDB.QueryContext(ctx, `
        SELECT m.id,
               COALESCE(strftime('%Y', m.sent_at), '') AS year,
               COALESCE(s.kind, '') AS source_kind,
               COALESCE(s.identifier, '') AS source_identifier,
               EXISTS (SELECT 1 FROM attachments a WHERE a.message_id = m.id) AS has_attachment,
               COALESCE(m.subject, ''),
               COALESCE(mb.body_text, ''),
               COALESCE(mb.body_html, '')
          FROM messages m
          LEFT JOIN sources s ON s.id = m.source_id
          LEFT JOIN message_bodies mb ON mb.message_id = m.id
         WHERE m.deleted_from_source_at IS NULL`)
	if err != nil {
		return fmt.Errorf("CreateStratifiedSample: candidate query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	type cand struct {
		ID           int64
		Year         string
		Kind         string
		Identifier   string
		HasAttach    bool
		LengthBucket string
	}
	var cands []cand
	for rows.Next() {
		var c cand
		var subject, bodyText, bodyHTML string
		if err := rows.Scan(&c.ID, &c.Year, &c.Kind, &c.Identifier, &c.HasAttach, &subject, &bodyText, &bodyHTML); err != nil {
			return err
		}
		body := bodyText
		if body == "" && bodyHTML != "" {
			body = mime.StripHTML(bodyHTML)
		}
		text, _ := embed.Preprocess(subject, body, maxInputChars, pp)
		chars := len([]rune(text))
		switch {
		case chars < 500:
			c.LengthBucket = "short"
		case chars < 5000:
			c.LengthBucket = "medium"
		default:
			c.LengthBucket = "long"
		}
		cands = append(cands, c)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	rng := rand.New(rand.NewSource(seed)) //nolint:gosec // deterministic benchmark use

	pick := func(bucket []cand, target int) []cand {
		if len(bucket) <= target {
			return bucket
		}
		rng.Shuffle(len(bucket), func(i, j int) { bucket[i], bucket[j] = bucket[j], bucket[i] })
		return bucket[:target]
	}

	type bucketDef struct {
		Axis   string
		Key    string
		Weight float64
		Match  func(cand) bool
	}
	var defs []bucketDef

	// addBuckets iterates the weights map in sorted key order so that the
	// sequence of rng draws is deterministic regardless of map-iteration order.
	addBuckets := func(axis string, weights map[string]float64, match func(cand, string) bool) {
		keys := make([]string, 0, len(weights))
		for k := range weights {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		for _, k := range keys {
			kk := k
			defs = append(defs, bucketDef{Axis: axis, Key: kk, Weight: weights[kk], Match: func(c cand) bool { return match(c, kk) }})
		}
	}

	if len(spec.Length) > 0 {
		addBuckets("length", spec.Length, func(c cand, k string) bool { return c.LengthBucket == k })
	}
	if len(spec.Year) > 0 {
		addBuckets("year", spec.Year, func(c cand, k string) bool { return c.Year == k })
	}
	if len(spec.Account) > 0 {
		addBuckets("account", spec.Account, func(c cand, k string) bool { return c.Identifier == k })
	}
	if len(spec.Attachments) > 0 {
		addBuckets("attachments", spec.Attachments, func(c cand, k string) bool {
			switch k {
			case "with":
				return c.HasAttach
			case "without":
				return !c.HasAttach
			}
			return false
		})
	}
	if len(spec.SourceType) > 0 {
		addBuckets("source_type", spec.SourceType, func(c cand, k string) bool { return c.Kind == k })
	}

	if len(defs) == 0 {
		// Unstratified: pick size uniformly.
		if len(cands) == 0 {
			return fmt.Errorf("CreateStratifiedSample: no candidate messages")
		}
		ids := make([]int64, len(cands))
		for i, c := range cands {
			ids[i] = c.ID
		}
		rng.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
		if size > len(ids) {
			size = len(ids)
		}
		ids = ids[:size]
		slices.SortFunc(ids, func(a, b int64) int { return cmp.Compare(a, b) })
		return CreateSampleFromIDs(ctx, vecDB, name, ids, SampleMeta{Seed: seed, Notes: notes})
	}

	type alloc struct {
		Def    bucketDef
		Bucket []cand
		Target int
		Frac   float64
	}
	allocs := make([]alloc, 0, len(defs))
	totalFloor := 0
	for _, d := range defs {
		var bucket []cand
		for _, c := range cands {
			if d.Match(c) {
				bucket = append(bucket, c)
			}
		}
		target := int(float64(size) * d.Weight)
		frac := float64(size)*d.Weight - float64(target)
		allocs = append(allocs, alloc{Def: d, Bucket: bucket, Target: target, Frac: frac})
		totalFloor += target
	}
	remainder := size - totalFloor
	sort.SliceStable(allocs, func(i, j int) bool { return allocs[i].Frac > allocs[j].Frac })
	for i := range remainder {
		allocs[i].Target++
	}
	var picked []int64
	var stratums []string
	for _, a := range allocs {
		if a.Target == 0 {
			continue
		}
		if len(a.Bucket) == 0 {
			return fmt.Errorf("stratum %q=%q: 0 candidate messages in this stratum", a.Def.Axis, a.Def.Key)
		}
		if len(a.Bucket) < a.Target {
			return fmt.Errorf("stratum %q=%q: only %d candidates available, requested %d", a.Def.Axis, a.Def.Key, len(a.Bucket), a.Target)
		}
		chosen := pick(a.Bucket, a.Target)
		for _, c := range chosen {
			picked = append(picked, c.ID)
			stratums = append(stratums, fmt.Sprintf("%s=%s", a.Def.Axis, a.Def.Key))
		}
	}

	type pair struct {
		ID      int64
		Stratum string
	}
	pairs := make([]pair, len(picked))
	for i := range picked {
		pairs[i] = pair{ID: picked[i], Stratum: stratums[i]}
	}
	slices.SortFunc(pairs, func(a, b pair) int { return cmp.Compare(a.ID, b.ID) })
	finalIDs := make([]int64, len(pairs))
	finalStrata := make([]string, len(pairs))
	for i, p := range pairs {
		finalIDs[i] = p.ID
		finalStrata[i] = p.Stratum
	}
	specJSON, _ := json.Marshal(spec)
	return CreateSampleFromIDs(ctx, vecDB, name, finalIDs, SampleMeta{Seed: seed, StratifySpec: string(specJSON), Notes: notes}, finalStrata)
}
