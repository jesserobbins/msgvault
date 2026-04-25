package bench

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/wesm/msgvault/internal/vector/embed"
)

func TestCreateSample_Basic(t *testing.T) {
	db := openMem(t)
	ctx := context.Background()
	if err := EnsureSchema(ctx, db); err != nil {
		t.Fatal(err)
	}
	ids := []int64{1, 2, 3, 4, 5}
	err := CreateSampleFromIDs(ctx, db, "demo", ids, SampleMeta{Seed: 42, Notes: "hand-picked"})
	if err != nil {
		t.Fatalf("CreateSampleFromIDs: %v", err)
	}
	got, err := SampleMessageIDs(ctx, db, "demo")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(ids) {
		t.Fatalf("ids: got %d, want %d", len(got), len(ids))
	}
}

func TestCreateSample_RejectsEmptyName(t *testing.T) {
	db := openMem(t)
	ctx := context.Background()
	err := CreateSampleFromIDs(ctx, db, "", []int64{1}, SampleMeta{})
	if err == nil {
		t.Fatal("empty name accepted")
	}
}

func TestCreateSample_DuplicateName(t *testing.T) {
	db := openMem(t)
	ctx := context.Background()
	if err := EnsureSchema(ctx, db); err != nil {
		t.Fatal(err)
	}
	if err := CreateSampleFromIDs(ctx, db, "x", []int64{1}, SampleMeta{}); err != nil {
		t.Fatal(err)
	}
	err := CreateSampleFromIDs(ctx, db, "x", []int64{2}, SampleMeta{})
	if err == nil {
		t.Fatal("duplicate name accepted")
	}
}

func TestCreateSample_StratumLengthMismatch(t *testing.T) {
	db := openMem(t)
	ctx := context.Background()
	if err := EnsureSchema(ctx, db); err != nil {
		t.Fatal(err)
	}
	err := CreateSampleFromIDs(ctx, db, "x", []int64{1, 2, 3}, SampleMeta{}, []string{"short"})
	if err == nil {
		t.Fatal("stratum length mismatch accepted")
	}
}

func TestSampleExists(t *testing.T) {
	db := openMem(t)
	ctx := context.Background()
	if err := EnsureSchema(ctx, db); err != nil {
		t.Fatal(err)
	}
	ok, err := SampleExists(ctx, db, "x")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Error("SampleExists true on empty DB")
	}
	if err := CreateSampleFromIDs(ctx, db, "x", []int64{1}, SampleMeta{}); err != nil {
		t.Fatal(err)
	}
	ok, err = SampleExists(ctx, db, "x")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Error("SampleExists false after create")
	}
}

func TestDeleteSample_Cascade(t *testing.T) {
	// openMem (in store_test.go) enables PRAGMA foreign_keys = ON.
	db := openMem(t)
	ctx := context.Background()
	if err := EnsureSchema(ctx, db); err != nil {
		t.Fatal(err)
	}
	if err := CreateSampleFromIDs(ctx, db, "x", []int64{1, 2, 3}, SampleMeta{}); err != nil {
		t.Fatal(err)
	}
	if err := DeleteSample(ctx, db, "x"); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM bench_sample_messages WHERE sample_name='x'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("cascade failed: %d rows remain", n)
	}
}

func TestListSamples_Order(t *testing.T) {
	db := openMem(t)
	ctx := context.Background()
	if err := EnsureSchema(ctx, db); err != nil {
		t.Fatal(err)
	}
	// Two samples; later-created should sort first.
	if err := CreateSampleFromIDs(ctx, db, "first", []int64{1}, SampleMeta{}); err != nil {
		t.Fatal(err)
	}
	// sleep impossible — we rely on insertion order being preserved
	// when created_at is identical; instead, force a different
	// created_at by direct UPDATE.
	if _, err := db.Exec(`UPDATE bench_samples SET created_at = 100 WHERE name = 'first'`); err != nil {
		t.Fatal(err)
	}
	if err := CreateSampleFromIDs(ctx, db, "second", []int64{2}, SampleMeta{}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE bench_samples SET created_at = 200 WHERE name = 'second'`); err != nil {
		t.Fatal(err)
	}
	summaries, err := ListSamples(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if len(summaries) != 2 {
		t.Fatalf("got %d samples, want 2", len(summaries))
	}
	if summaries[0].Name != "second" {
		t.Errorf("expected 'second' first (newest), got %q", summaries[0].Name)
	}
}

func TestListSamples_RequiresSchema(t *testing.T) {
	db := openMem(t)
	_, err := ListSamples(context.Background(), db)
	if err == nil {
		t.Fatal("ListSamples on empty DB should error")
	}
}

func TestParseStratifySpec_ExplicitWeights(t *testing.T) {
	s, err := ParseStratifySpec("length=short:25%,medium:50%,long:25%")
	if err != nil {
		t.Fatal(err)
	}
	if got := s.Length["short"]; got < 0.249 || got > 0.251 {
		t.Errorf("short weight: got %v, want 0.25", got)
	}
	if got := s.Length["medium"]; got < 0.499 || got > 0.501 {
		t.Errorf("medium weight: got %v, want 0.50", got)
	}
	if got := s.Length["long"]; got < 0.249 || got > 0.251 {
		t.Errorf("long weight: got %v, want 0.25", got)
	}
}

func TestParseStratifySpec_ImplicitEqualWeights(t *testing.T) {
	s, err := ParseStratifySpec("year=2014,2015,2016,2017")
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"2014", "2015", "2016", "2017"} {
		if got := s.Year[k]; got < 0.249 || got > 0.251 {
			t.Errorf("year %s: got %v, want 0.25", k, got)
		}
	}
}

func TestParseStratifySpec_RejectsMultipleAxes(t *testing.T) {
	_, err := ParseStratifySpec("length=short:30%,long:70%;attachments=with:50%,without:50%")
	if err == nil {
		t.Fatal("expected error: multi-axis stratification is not supported")
	}
}

func TestParseStratifySpec_RejectsMixedWeights(t *testing.T) {
	_, err := ParseStratifySpec("length=short:25%,medium")
	if err == nil {
		t.Fatal("expected error for mixed explicit/implicit")
	}
}

func TestParseStratifySpec_RejectsBadSum(t *testing.T) {
	_, err := ParseStratifySpec("length=short:50%,medium:30%") // sums to 0.80
	if err == nil {
		t.Fatal("expected error: weights don't sum to 1")
	}
}

func TestParseStratifySpec_RejectsUnknownAxis(t *testing.T) {
	_, err := ParseStratifySpec("color=red:50%,blue:50%")
	if err == nil {
		t.Fatal("expected error for unknown axis")
	}
}

func TestParseStratifySpec_Empty(t *testing.T) {
	s, err := ParseStratifySpec("")
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Length)+len(s.Year)+len(s.Account)+len(s.Attachments)+len(s.SourceType) != 0 {
		t.Errorf("empty spec should have no buckets")
	}
}

// CreateStratifiedSample integration test.
func TestCreateStratifiedSample_Length(t *testing.T) {
	vecDB := openMem(t)
	if err := EnsureSchema(context.Background(), vecDB); err != nil {
		t.Fatal(err)
	}
	mainDB := openSampleMainDB(t, 1000) // helper below
	spec := StratifySpec{
		Length: map[string]float64{"short": 0.25, "medium": 0.50, "long": 0.25},
	}
	pp := embed.PreprocessConfig{}
	err := CreateStratifiedSample(context.Background(), vecDB, mainDB, "demo", 100, spec, 42, pp, 32768, "")
	if err != nil {
		t.Fatalf("CreateStratifiedSample: %v", err)
	}
	// Count strata in the produced sample.
	rows, err := vecDB.Query(`SELECT stratum, COUNT(*) FROM bench_sample_messages WHERE sample_name = ? GROUP BY stratum`, "demo")
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for rows.Next() {
		var s string
		var n int
		if err := rows.Scan(&s, &n); err != nil {
			t.Fatal(err)
		}
		counts[s] = n
	}
	_ = rows.Close()
	short := counts["length=short"]
	medium := counts["length=medium"]
	long := counts["length=long"]
	if short+medium+long != 100 {
		t.Errorf("total: %d, want 100", short+medium+long)
	}
	// Allow ±1 from rounding remainders.
	if short < 24 || short > 26 {
		t.Errorf("short: got %d, want ~25", short)
	}
	if medium < 49 || medium > 51 {
		t.Errorf("medium: got %d, want ~50", medium)
	}
	if long < 24 || long > 26 {
		t.Errorf("long: got %d, want ~25", long)
	}
}

func TestCreateStratifiedSample_SeedReproducibility(t *testing.T) {
	spec := StratifySpec{Length: map[string]float64{"short": 1.0}}
	pp := embed.PreprocessConfig{}

	vec1 := openMem(t)
	_ = EnsureSchema(context.Background(), vec1)
	main := openSampleMainDB(t, 200)
	_ = CreateStratifiedSample(context.Background(), vec1, main, "a", 10, spec, 99, pp, 32768, "")
	ids1, _ := SampleMessageIDs(context.Background(), vec1, "a")

	vec2 := openMem(t)
	_ = EnsureSchema(context.Background(), vec2)
	_ = CreateStratifiedSample(context.Background(), vec2, main, "b", 10, spec, 99, pp, 32768, "")
	ids2, _ := SampleMessageIDs(context.Background(), vec2, "b")

	if len(ids1) != len(ids2) {
		t.Fatalf("len mismatch: %d vs %d", len(ids1), len(ids2))
	}
	for i := range ids1 {
		if ids1[i] != ids2[i] {
			t.Fatalf("seed reproducibility broken at i=%d: %d vs %d", i, ids1[i], ids2[i])
		}
	}
}

func TestCreateStratifiedSample_EmptyStratum(t *testing.T) {
	vecDB := openMem(t)
	_ = EnsureSchema(context.Background(), vecDB)
	mainDB := openSampleMainDB(t, 50)
	spec := StratifySpec{
		SourceType: map[string]float64{"imessage": 1.0}, // not present in fixture
	}
	pp := embed.PreprocessConfig{}
	err := CreateStratifiedSample(context.Background(), vecDB, mainDB, "demo", 10, spec, 1, pp, 32768, "")
	if err == nil {
		t.Fatal("expected empty-stratum error")
	}
}

func TestCreateStratifiedSample_Unstratified(t *testing.T) {
	vecDB := openMem(t)
	_ = EnsureSchema(context.Background(), vecDB)
	mainDB := openSampleMainDB(t, 50)
	err := CreateStratifiedSample(context.Background(), vecDB, mainDB, "demo", 10, StratifySpec{}, 1, embed.PreprocessConfig{}, 32768, "")
	if err != nil {
		t.Fatalf("unstratified CreateStratifiedSample: %v", err)
	}
	ids, _ := SampleMessageIDs(context.Background(), vecDB, "demo")
	if len(ids) != 10 {
		t.Errorf("got %d ids, want 10", len(ids))
	}
}

// openSampleMainDB creates an in-memory main DB with n messages
// distributed across length buckets. Roughly 25% short, 50% medium,
// 25% long bodies.
func openSampleMainDB(t *testing.T, n int) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`
CREATE TABLE messages (id INTEGER PRIMARY KEY, subject TEXT, source_id INTEGER, sent_at DATETIME, deleted_from_source_at DATETIME);
CREATE TABLE message_bodies (message_id INTEGER PRIMARY KEY, body_text TEXT, body_html TEXT);
CREATE TABLE attachments (message_id INTEGER, hash TEXT);
CREATE TABLE sources (id INTEGER PRIMARY KEY, kind TEXT, identifier TEXT);
INSERT INTO sources (id, kind, identifier) VALUES (1, 'email', 'one@example.test'), (2, 'email', 'two@example.test');`); err != nil {
		t.Fatal(err)
	}
	short := strings.Repeat("a", 100)
	medium := strings.Repeat("b", 1500)
	long := strings.Repeat("c", 6000)
	for i := 1; i <= n; i++ {
		var body string
		switch i % 4 {
		case 0:
			body = long
		case 1:
			body = short
		default:
			body = medium
		}
		sourceID := 1 + (i % 2)
		year := 2014 + (i % 5)
		sentAt := fmt.Sprintf("%d-06-01 12:00:00", year)
		if _, err := db.Exec(`INSERT INTO messages (id, subject, source_id, sent_at) VALUES (?, '', ?, ?)`, i, sourceID, sentAt); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO message_bodies (message_id, body_text) VALUES (?, ?)`, i, body); err != nil {
			t.Fatal(err)
		}
		if i%3 == 0 {
			if _, err := db.Exec(`INSERT INTO attachments (message_id, hash) VALUES (?, 'h')`, i); err != nil {
				t.Fatal(err)
			}
		}
	}
	return db
}
