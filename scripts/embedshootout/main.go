//go:build sqlite_vec

package main

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"

	"github.com/wesm/msgvault/internal/vector"
	"github.com/wesm/msgvault/internal/vector/embed"
	"github.com/wesm/msgvault/internal/vector/embed/bench"
	"github.com/wesm/msgvault/internal/vector/sqlitevec"
)

var logger *slog.Logger

func main() {
	logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	verb := os.Args[1]
	args := os.Args[2:]
	var err error
	switch verb {
	case "sample-create":
		err = cmdSampleCreate(args)
	case "sample-list":
		err = cmdSampleList(args)
	case "sample-show":
		err = cmdSampleShow(args)
	case "sample-delete":
		err = cmdSampleDelete(args)
	case "run":
		err = cmdRun(args)
	case "sweep":
		err = cmdSweep(args)
	case "list":
		err = cmdList(args)
	case "show":
		err = cmdShow(args)
	case "compare":
		err = cmdCompare(args)
	case "delete":
		err = cmdDelete(args)
	case "-h", "--help", "help":
		usage()
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown verb %q\n", verb)
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `embedshootout — embedding-performance harness for msgvault

Usage: embedshootout <verb> [flags]

Verbs:
  sample-create, sample-list, sample-show, sample-delete
  run, sweep
  list, show, compare, delete
  help

Run 'embedshootout <verb> -h' for verb-specific flags.`)
}

// home returns MSGVAULT_HOME or ~/.msgvault.
func home() string {
	if h := os.Getenv("MSGVAULT_HOME"); h != "" {
		return h
	}
	h, err := os.UserHomeDir()
	if err != nil {
		return ".msgvault"
	}
	return filepath.Join(h, ".msgvault")
}

func defaultVectorsDB() string { return filepath.Join(home(), "vectors.db") }
func defaultMainDB() string    { return filepath.Join(home(), "msgvault.db") }

// openDBs opens vectors.db (read-write) and msgvault.db (read-only).
func openDBs(vectorsPath, mainPath string) (*sqlitevec.Backend, *sql.DB, *sql.DB, error) {
	if err := sqlitevec.RegisterExtension(); err != nil {
		return nil, nil, nil, fmt.Errorf("RegisterExtension: %w", err)
	}
	mainDB, err := sql.Open(sqlitevec.DriverName(), mainPath+"?mode=ro")
	if err != nil {
		return nil, nil, nil, fmt.Errorf("open main: %w", err)
	}
	b, err := sqlitevec.Open(context.Background(), sqlitevec.Options{
		Path:      vectorsPath,
		MainPath:  mainPath,
		Dimension: 768,
		MainDB:    mainDB,
	})
	if err != nil {
		_ = mainDB.Close()
		return nil, nil, nil, fmt.Errorf("open vectors backend: %w", err)
	}
	return b, b.DB(), mainDB, nil
}

func asBenchBackend(b vector.Backend) (vector.BenchBackend, error) {
	bb, ok := b.(vector.BenchBackend)
	if !ok {
		return nil, fmt.Errorf("backend does not implement BenchBackend (rebuild with -tags sqlite_vec)")
	}
	return bb, nil
}

func opportunisticCleanup(ctx context.Context, db *sql.DB, bb vector.BenchBackend) {
	n, err := bench.CleanupOrphanGenerations(ctx, db, bb, time.Hour)
	if err != nil {
		logger.Warn("orphan cleanup failed", "err", err)
		return
	}
	if n > 0 {
		logger.Info("dropped orphan bench gens", "count", n)
	}
}

// gitShortSHA returns the short HEAD SHA, or "" on any failure.
func gitShortSHA() string {
	out, err := exec.Command("git", "rev-parse", "--short", "HEAD").Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

// parseRunIDs parses a comma-separated list of int64 run IDs.
func parseRunIDs(s string) ([]int64, error) {
	parts := strings.Split(s, ",")
	ids := make([]int64, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		id, err := strconv.ParseInt(p, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid run ID %q: %w", p, err)
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// ---------- sample-create ----------

func cmdSampleCreate(args []string) error {
	fs := flag.NewFlagSet("sample-create", flag.ExitOnError)
	name := fs.String("name", "", "sample name (required)")
	size := fs.Int("size", 0, "number of messages to sample (required)")
	stratify := fs.String("stratify", "", "stratify spec, e.g. length=short:25%,medium:50%,long:25% (single axis only)")
	seed := fs.Int64("seed", time.Now().UnixNano(), "random seed")
	notes := fs.String("notes", "", "freeform notes")
	vectorsDB := fs.String("vectors-db", defaultVectorsDB(), "path to vectors.db")
	mainDB := fs.String("main-db", defaultMainDB(), "path to msgvault.db")
	_ = fs.Parse(args)

	if *name == "" {
		return fmt.Errorf("-name is required")
	}
	if *size <= 0 {
		return fmt.Errorf("-size must be > 0")
	}

	b, vecDB, mDB, err := openDBs(*vectorsDB, *mainDB)
	if err != nil {
		return err
	}
	defer func() { _ = b.Close(); _ = mDB.Close() }()

	ctx := context.Background()
	bb, err := asBenchBackend(b)
	if err != nil {
		return err
	}
	if err := bench.EnsureSchema(ctx, vecDB); err != nil {
		return err
	}
	opportunisticCleanup(ctx, vecDB, bb)

	spec, err := bench.ParseStratifySpec(*stratify)
	if err != nil {
		return fmt.Errorf("parse stratify: %w", err)
	}
	pp := embed.PreprocessConfig{StripQuotes: true, StripSignatures: true}
	err = bench.CreateStratifiedSample(ctx, vecDB, mDB, *name, *size, spec, *seed, pp, 8192, *notes)
	if err != nil {
		return fmt.Errorf("create sample: %w", err)
	}
	fmt.Printf("created sample %q (%d messages)\n", *name, *size)
	return nil
}

// ---------- sample-list ----------

func cmdSampleList(args []string) error {
	fs := flag.NewFlagSet("sample-list", flag.ExitOnError)
	vectorsDB := fs.String("vectors-db", defaultVectorsDB(), "path to vectors.db")
	_ = fs.Parse(args)

	b, vecDB, mDB, err := openDBs(*vectorsDB, defaultMainDB())
	if err != nil {
		return err
	}
	defer func() { _ = b.Close(); _ = mDB.Close() }()

	ctx := context.Background()
	if err := bench.RequireSchema(ctx, vecDB); err != nil {
		if errors.Is(err, bench.ErrNoBenchData) {
			fmt.Fprintln(os.Stderr, "no bench data yet (run sample-create or a benchmark first)")
			return nil
		}
		return err
	}
	samples, err := bench.ListSamples(ctx, vecDB)
	if err != nil {
		return err
	}
	if len(samples) == 0 {
		fmt.Println("(no samples)")
		return nil
	}
	for _, s := range samples {
		ts := time.Unix(s.CreatedAt, 0).UTC().Format(time.RFC3339)
		fmt.Printf("%-30s  size=%-6d  created=%s", s.Name, s.Size, ts)
		if s.Notes != "" {
			fmt.Printf("  notes=%q", s.Notes)
		}
		fmt.Println()
	}
	return nil
}

// ---------- sample-show ----------

func cmdSampleShow(args []string) error {
	fs := flag.NewFlagSet("sample-show", flag.ExitOnError)
	name := fs.String("name", "", "sample name (required)")
	vectorsDB := fs.String("vectors-db", defaultVectorsDB(), "path to vectors.db")
	_ = fs.Parse(args)

	if *name == "" {
		return fmt.Errorf("-name is required")
	}

	b, vecDB, mDB, err := openDBs(*vectorsDB, defaultMainDB())
	if err != nil {
		return err
	}
	defer func() { _ = b.Close(); _ = mDB.Close() }()

	ctx := context.Background()
	if err := bench.RequireSchema(ctx, vecDB); err != nil {
		if errors.Is(err, bench.ErrNoBenchData) {
			fmt.Fprintln(os.Stderr, "no bench data yet (run sample-create or a benchmark first)")
			return nil
		}
		return err
	}
	samples, err := bench.ListSamples(ctx, vecDB)
	if err != nil {
		return err
	}
	for _, s := range samples {
		if s.Name == *name {
			ts := time.Unix(s.CreatedAt, 0).UTC().Format(time.RFC3339)
			fmt.Printf("name:     %s\n", s.Name)
			fmt.Printf("size:     %d\n", s.Size)
			fmt.Printf("created:  %s\n", ts)
			fmt.Printf("seed:     %d\n", s.Seed)
			if s.StratifySpec != "" {
				fmt.Printf("stratify: %s\n", s.StratifySpec)
			}
			if s.Notes != "" {
				fmt.Printf("notes:    %s\n", s.Notes)
			}
			return nil
		}
	}
	return fmt.Errorf("sample %q not found", *name)
}

// ---------- sample-delete ----------

func cmdSampleDelete(args []string) error {
	fs := flag.NewFlagSet("sample-delete", flag.ExitOnError)
	name := fs.String("name", "", "sample name (required)")
	vectorsDB := fs.String("vectors-db", defaultVectorsDB(), "path to vectors.db")
	_ = fs.Parse(args)

	if *name == "" {
		return fmt.Errorf("-name is required")
	}

	b, vecDB, mDB, err := openDBs(*vectorsDB, defaultMainDB())
	if err != nil {
		return err
	}
	defer func() { _ = b.Close(); _ = mDB.Close() }()

	ctx := context.Background()
	bb, err := asBenchBackend(b)
	if err != nil {
		return err
	}
	if err := bench.EnsureSchema(ctx, vecDB); err != nil {
		return err
	}
	opportunisticCleanup(ctx, vecDB, bb)

	if err := bench.DeleteSample(ctx, vecDB, *name); err != nil {
		return fmt.Errorf("delete sample: %w", err)
	}
	fmt.Printf("deleted sample %q\n", *name)
	return nil
}

// ---------- run ----------

func cmdRun(args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	mode := fs.String("mode", "endpoint", "endpoint or pipeline")
	sampleName := fs.String("sample", "", "sample name (required)")
	endpoint := fs.String("endpoint", "", "embedding endpoint URL including /v1 (required for endpoint mode)")
	model := fs.String("model", "", "model name (required)")
	dimension := fs.Int("dimension", 768, "vector dimension")
	batchSize := fs.Int("batch-size", 16, "batch size")
	workers := fs.Int("workers", 1, "number of workers")
	warmup := fs.Int("warmup-batches", 1, "warmup batches")
	apiKey := fs.String("api-key", "", "API key / bearer token")
	maxChars := fs.Int("max-input-chars", 8192, "max input chars per message")
	vectorsDB := fs.String("vectors-db", defaultVectorsDB(), "path to vectors.db")
	mainDB := fs.String("main-db", defaultMainDB(), "path to msgvault.db")
	_ = fs.Parse(args)

	if *sampleName == "" {
		return fmt.Errorf("-sample is required")
	}
	if *model == "" {
		return fmt.Errorf("-model is required")
	}
	if *endpoint == "" {
		return fmt.Errorf("-endpoint is required (both endpoint and pipeline modes call the embedding API)")
	}

	b, vecDB, mDB, err := openDBs(*vectorsDB, *mainDB)
	if err != nil {
		return err
	}
	defer func() { _ = b.Close(); _ = mDB.Close() }()

	ctx := context.Background()
	bb, err := asBenchBackend(b)
	if err != nil {
		return err
	}
	if err := bench.EnsureSchema(ctx, vecDB); err != nil {
		return err
	}
	opportunisticCleanup(ctx, vecDB, bb)

	client := embed.NewClient(embed.Config{
		Endpoint:  *endpoint,
		APIKey:    *apiKey,
		Model:     *model,
		Dimension: *dimension,
	})
	pp := embed.PreprocessConfig{StripQuotes: true, StripSignatures: true}
	in := bench.RunCellInputs{
		SweepID:       nil,
		SampleName:    *sampleName,
		Mode:          *mode,
		Cell:          bench.Cell{"model": *model, "dimension": int64(*dimension)},
		BackendBase:   b,
		MainDB:        mDB,
		VectorsDB:     vecDB,
		Client:        client,
		Preprocess:    pp,
		MaxInputChars: *maxChars,
		BatchSize:     *batchSize,
		Workers:       *workers,
		WarmupBatches: *warmup,
		Model:         *model,
		Dimension:     *dimension,
	}
	runID, err := bench.RunCell(ctx, in)
	if err != nil {
		return fmt.Errorf("run: %w", err)
	}
	fmt.Printf("run %d complete\n", runID)
	return nil
}

// ---------- sweep ----------

func cmdSweep(args []string) error {
	fs := flag.NewFlagSet("sweep", flag.ExitOnError)
	planFile := fs.String("plan", "", "path to TOML plan file (if omitted, use flag axes)")
	sampleName := fs.String("sample", "", "sample name")
	mode := fs.String("mode", "endpoint", "endpoint or pipeline")
	endpointsList := fs.String("endpoints", "", "comma-separated endpoint URLs")
	modelsList := fs.String("models", "", "comma-separated model names")
	dimensionsList := fs.String("dimensions", "", "comma-separated dimensions")
	batchSizesList := fs.String("batch-sizes", "", "comma-separated batch sizes")
	workersList := fs.String("workers", "", "comma-separated worker counts")
	warmup := fs.Int("warmup-batches", 1, "warmup batches")
	apiKey := fs.String("api-key", "", "API key / bearer token")
	maxChars := fs.Int("max-input-chars", 8192, "max input chars per message")
	notes := fs.String("notes", "", "freeform notes")
	vectorsDB := fs.String("vectors-db", defaultVectorsDB(), "path to vectors.db")
	mainDB := fs.String("main-db", defaultMainDB(), "path to msgvault.db")
	_ = fs.Parse(args)

	b, vecDB, mDB, err := openDBs(*vectorsDB, *mainDB)
	if err != nil {
		return err
	}
	defer func() { _ = b.Close(); _ = mDB.Close() }()

	ctx := context.Background()
	bb, err := asBenchBackend(b)
	if err != nil {
		return err
	}
	if err := bench.EnsureSchema(ctx, vecDB); err != nil {
		return err
	}
	opportunisticCleanup(ctx, vecDB, bb)

	var plan *bench.Plan
	var planTOML string
	var planPath string

	if *planFile != "" {
		// Plan-file path.
		data, readErr := os.ReadFile(*planFile)
		if readErr != nil {
			return fmt.Errorf("read plan: %w", readErr)
		}
		plan, err = bench.ParsePlan(data)
		if err != nil {
			return fmt.Errorf("parse plan: %w", err)
		}
		if err := validatePlanHasEndpoint(plan); err != nil {
			return err
		}
		planTOML = string(data)
		planPath = *planFile
		if *sampleName != "" {
			plan.Sample = *sampleName
		}
	} else {
		// Flags-only path: synthesize a Plan.
		if *sampleName == "" {
			return fmt.Errorf("-sample is required when -plan is not given")
		}
		plan, err = synthesizePlan(*sampleName, *mode, *endpointsList, *modelsList,
			*dimensionsList, *batchSizesList, *workersList, *warmup, *notes)
		if err != nil {
			return err
		}
		// Serialize plan to TOML and write to auto file.
		var buf bytes.Buffer
		if encErr := toml.NewEncoder(&buf).Encode(plan); encErr != nil {
			return fmt.Errorf("encode plan: %w", encErr)
		}
		planTOML = buf.String()

		autoDir := filepath.Join(home(), "sweeps", "auto")
		if mkErr := os.MkdirAll(autoDir, 0o755); mkErr != nil {
			return fmt.Errorf("create auto sweep dir: %w", mkErr)
		}
		ts := time.Now().UTC().Format(time.RFC3339)
		// RFC3339 colons are invalid in filenames on some filesystems; replace.
		safets := strings.ReplaceAll(ts, ":", "-")
		planPath = filepath.Join(autoDir, safets+".toml")
		if writeErr := os.WriteFile(planPath, buf.Bytes(), 0o644); writeErr != nil {
			return fmt.Errorf("write auto plan: %w", writeErr)
		}
		logger.Info("wrote auto plan", "path", planPath)
	}

	theAPIKey := *apiKey
	pp := embed.PreprocessConfig{StripQuotes: true, StripSignatures: true}
	theMaxChars := *maxChars
	theSample := plan.Sample

	fixedEndpoint, _ := plan.Fixed["endpoint"].(string)

	cellRunner := func(ctx context.Context, sweepID int64, cell bench.Cell) (bench.RunCellInputs, error) {
		endpointVal, _ := cell["endpoint"].(string)
		if endpointVal == "" {
			endpointVal = fixedEndpoint
		}
		modelVal, _ := cell["model"].(string)
		var dimVal int
		switch v := cell["dimension"].(type) {
		case int64:
			dimVal = int(v)
		case int:
			dimVal = v
		case float64:
			dimVal = int(v)
		}
		if dimVal == 0 {
			dimVal = 768
		}
		var bsVal int
		switch v := cell["batch_size"].(type) {
		case int64:
			bsVal = int(v)
		case int:
			bsVal = v
		case float64:
			bsVal = int(v)
		}
		if bsVal == 0 {
			bsVal = 16
		}
		var wVal int
		switch v := cell["workers"].(type) {
		case int64:
			wVal = int(v)
		case int:
			wVal = v
		case float64:
			wVal = int(v)
		}
		if wVal == 0 {
			wVal = 1
		}

		client := embed.NewClient(embed.Config{
			Endpoint:  endpointVal,
			APIKey:    theAPIKey,
			Model:     modelVal,
			Dimension: dimVal,
		})
		sid := sweepID
		return bench.RunCellInputs{
			SweepID:       &sid,
			SampleName:    theSample,
			Mode:          plan.Mode,
			Cell:          cell,
			BackendBase:   b,
			MainDB:        mDB,
			VectorsDB:     vecDB,
			Client:        client,
			Preprocess:    pp,
			MaxInputChars: theMaxChars,
			BatchSize:     bsVal,
			Workers:       wVal,
			WarmupBatches: plan.WarmupBatches,
			Model:         modelVal,
			Dimension:     dimVal,
		}, nil
	}

	sweepID, err := bench.RunSweep(ctx, vecDB, plan, plan.Sample, planTOML, planPath, gitShortSHA(), *notes, cellRunner)
	if err != nil {
		return fmt.Errorf("sweep: %w", err)
	}
	fmt.Printf("sweep %d complete\n", sweepID)
	return nil
}

// validatePlanHasEndpoint checks that a plan supplies an embedding
// endpoint URL via either an "endpoint" matrix axis or
// Fixed["endpoint"]. Without this guard, plan-file invocations with
// no endpoint produce N per-worker request errors against an empty
// URL instead of one upfront rejection.
func validatePlanHasEndpoint(plan *bench.Plan) error {
	for _, ax := range plan.Matrix {
		if ax.Name == "endpoint" && len(ax.Values) > 0 {
			return nil
		}
	}
	if v, ok := plan.Fixed["endpoint"].(string); ok && v != "" {
		return nil
	}
	return fmt.Errorf("plan has no embedding endpoint (add an `endpoint` matrix axis or set `fixed.endpoint`)")
}

// synthesizePlan builds a *bench.Plan from the flag-values for the flags-only sweep path.
func synthesizePlan(sample, mode, endpoints, models, dimensions, batchSizes, workers string, warmup int, notes string) (*bench.Plan, error) {
	if strings.TrimSpace(endpoints) == "" {
		return nil, fmt.Errorf("-endpoints is required (a sweep needs at least one embedding endpoint URL)")
	}
	plan := &bench.Plan{
		Sample:        sample,
		Mode:          mode,
		WarmupBatches: warmup,
		Notes:         notes,
		Fixed:         map[string]any{},
	}

	addAxis := func(name, list string) {
		if list == "" {
			return
		}
		parts := strings.Split(list, ",")
		vals := make([]any, 0, len(parts))
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p != "" {
				vals = append(vals, p)
			}
		}
		if len(vals) > 0 {
			plan.Matrix = append(plan.Matrix, bench.MatrixAxis{Name: name, Values: vals})
		}
	}

	addIntAxis := func(name, list string) error {
		if list == "" {
			return nil
		}
		parts := strings.Split(list, ",")
		vals := make([]any, 0, len(parts))
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p == "" {
				continue
			}
			n, err := strconv.ParseInt(p, 10, 64)
			if err != nil {
				return fmt.Errorf("axis %q: invalid int %q: %w", name, p, err)
			}
			vals = append(vals, n)
		}
		if len(vals) > 0 {
			plan.Matrix = append(plan.Matrix, bench.MatrixAxis{Name: name, Values: vals})
		}
		return nil
	}

	addAxis("endpoint", endpoints)
	addAxis("model", models)
	if err := addIntAxis("dimension", dimensions); err != nil {
		return nil, err
	}
	if err := addIntAxis("batch_size", batchSizes); err != nil {
		return nil, err
	}
	if err := addIntAxis("workers", workers); err != nil {
		return nil, err
	}

	if len(plan.Matrix) == 0 {
		return nil, fmt.Errorf("at least one axis flag required (-endpoints, -models, -dimensions, -batch-sizes, or -workers)")
	}
	return plan, nil
}

// ---------- list ----------

func cmdList(args []string) error {
	fs := flag.NewFlagSet("list", flag.ExitOnError)
	sweepID := fs.Int64("sweep", 0, "filter to sweep ID (0 = all)")
	asJSON := fs.Bool("json", false, "output JSON")
	limit := fs.Int("limit", 50, "max runs to show")
	vectorsDB := fs.String("vectors-db", defaultVectorsDB(), "path to vectors.db")
	_ = fs.Parse(args)

	b, vecDB, mDB, err := openDBs(*vectorsDB, defaultMainDB())
	if err != nil {
		return err
	}
	defer func() { _ = b.Close(); _ = mDB.Close() }()

	ctx := context.Background()
	if err := bench.RequireSchema(ctx, vecDB); err != nil {
		if errors.Is(err, bench.ErrNoBenchData) {
			fmt.Fprintln(os.Stderr, "no bench data yet")
			return nil
		}
		return err
	}

	var sid *int64
	if *sweepID != 0 {
		sid = sweepID
	}
	runs, err := bench.ListRecentRuns(ctx, vecDB, sid, *limit)
	if err != nil {
		return err
	}
	if *asJSON {
		return bench.RenderListJSON(os.Stdout, runs)
	}
	return bench.RenderList(os.Stdout, runs)
}

// ---------- show ----------

func cmdShow(args []string) error {
	fs := flag.NewFlagSet("show", flag.ExitOnError)
	runID := fs.Int64("run", 0, "run ID (required)")
	asJSON := fs.Bool("json", false, "output JSON")
	vectorsDB := fs.String("vectors-db", defaultVectorsDB(), "path to vectors.db")
	_ = fs.Parse(args)

	if *runID == 0 {
		return fmt.Errorf("-run is required")
	}

	b, vecDB, mDB, err := openDBs(*vectorsDB, defaultMainDB())
	if err != nil {
		return err
	}
	defer func() { _ = b.Close(); _ = mDB.Close() }()

	ctx := context.Background()
	if err := bench.RequireSchema(ctx, vecDB); err != nil {
		if errors.Is(err, bench.ErrNoBenchData) {
			fmt.Fprintln(os.Stderr, "no bench data yet")
			return nil
		}
		return err
	}

	run, err := bench.LoadRun(ctx, vecDB, *runID)
	if err != nil {
		return fmt.Errorf("load run %d: %w", *runID, err)
	}
	if *asJSON {
		return bench.RenderShowJSON(os.Stdout, run)
	}
	return bench.RenderShow(os.Stdout, run)
}

// ---------- compare ----------

func cmdCompare(args []string) error {
	fs := flag.NewFlagSet("compare", flag.ExitOnError)
	runsFlag := fs.String("runs", "", "comma-separated run IDs (required)")
	asJSON := fs.Bool("json", false, "output JSON")
	vectorsDB := fs.String("vectors-db", defaultVectorsDB(), "path to vectors.db")
	_ = fs.Parse(args)

	if *runsFlag == "" {
		return fmt.Errorf("-runs is required")
	}
	ids, err := parseRunIDs(*runsFlag)
	if err != nil {
		return err
	}

	b, vecDB, mDB, err := openDBs(*vectorsDB, defaultMainDB())
	if err != nil {
		return err
	}
	defer func() { _ = b.Close(); _ = mDB.Close() }()

	ctx := context.Background()
	if err := bench.RequireSchema(ctx, vecDB); err != nil {
		if errors.Is(err, bench.ErrNoBenchData) {
			fmt.Fprintln(os.Stderr, "no bench data yet")
			return nil
		}
		return err
	}

	runs, err := bench.LoadRuns(ctx, vecDB, ids)
	if err != nil {
		return fmt.Errorf("load runs: %w", err)
	}
	if *asJSON {
		return bench.RenderCompareJSON(os.Stdout, runs)
	}
	return bench.RenderCompare(os.Stdout, runs)
}

// ---------- delete ----------

func cmdDelete(args []string) error {
	fs := flag.NewFlagSet("delete", flag.ExitOnError)
	runID := fs.Int64("run", 0, "delete run by ID")
	sweepID := fs.Int64("sweep", 0, "delete sweep by ID (cascades to runs and batches)")
	before := fs.String("before", "", "delete all sweeps started before YYYY-MM-DD")
	vectorsDB := fs.String("vectors-db", defaultVectorsDB(), "path to vectors.db")
	_ = fs.Parse(args)

	b, vecDB, mDB, err := openDBs(*vectorsDB, defaultMainDB())
	if err != nil {
		return err
	}
	defer func() { _ = b.Close(); _ = mDB.Close() }()

	ctx := context.Background()
	bb, err := asBenchBackend(b)
	if err != nil {
		return err
	}
	if err := bench.EnsureSchema(ctx, vecDB); err != nil {
		return err
	}
	opportunisticCleanup(ctx, vecDB, bb)

	switch {
	case *runID != 0:
		res, delErr := vecDB.ExecContext(ctx, `DELETE FROM bench_runs WHERE id = ?`, *runID)
		if delErr != nil {
			return fmt.Errorf("delete run: %w", delErr)
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return fmt.Errorf("no run with id %d", *runID)
		}
		fmt.Printf("deleted run %d\n", *runID)
	case *sweepID != 0:
		res, delErr := vecDB.ExecContext(ctx, `DELETE FROM bench_sweeps WHERE id = ?`, *sweepID)
		if delErr != nil {
			return fmt.Errorf("delete sweep: %w", delErr)
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return fmt.Errorf("no sweep with id %d", *sweepID)
		}
		fmt.Printf("deleted sweep %d\n", *sweepID)
	case *before != "":
		t, parseErr := time.Parse("2006-01-02", *before)
		if parseErr != nil {
			return fmt.Errorf("parse -before %q: %w", *before, parseErr)
		}
		cutoff := t.Unix()
		res, delErr := vecDB.ExecContext(ctx, `DELETE FROM bench_sweeps WHERE started_at < ?`, cutoff)
		if delErr != nil {
			return fmt.Errorf("delete before: %w", delErr)
		}
		n, _ := res.RowsAffected()
		fmt.Printf("deleted %d sweep(s) started before %s\n", n, *before)
	default:
		return fmt.Errorf("one of -run, -sweep, or -before is required")
	}
	return nil
}
