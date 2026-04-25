package bench

import (
	"reflect"
	"strings"
	"testing"
)

const planMinimal = `
sample = "demo"
mode = "endpoint"

[[matrix]]
name = "endpoint"
values = ["http://a", "http://b"]

[[matrix]]
name = "model"
values = ["x", "y"]

[[matrix]]
name = "dimension"
values = [768, 1536]

[[matrix]]
name = "batch_size"
values = [16, 32]

[[matrix]]
name = "workers"
values = [1, 2]
`

func TestParsePlan_Defaults(t *testing.T) {
	p, err := ParsePlan([]byte(planMinimal))
	if err != nil {
		t.Fatal(err)
	}
	if p.Mode != "endpoint" {
		t.Errorf("Mode: got %q, want endpoint", p.Mode)
	}
	if p.WarmupBatches != 1 {
		t.Errorf("WarmupBatches default: got %d, want 1", p.WarmupBatches)
	}
}

func TestParsePlan_MatrixCells(t *testing.T) {
	p, err := ParsePlan([]byte(planMinimal))
	if err != nil {
		t.Fatal(err)
	}
	cells, err := p.Cells()
	if err != nil {
		t.Fatal(err)
	}
	// 2 endpoints × 2 (model+dim zipped) × 2 batch × 2 workers = 16
	if len(cells) != 16 {
		t.Errorf("cells: got %d, want 16", len(cells))
	}
}

func TestParsePlan_ModelDimensionZip_MismatchedLen(t *testing.T) {
	bad := strings.Replace(planMinimal, `values = [768, 1536]`, `values = [768]`, 1)
	p, err := ParsePlan([]byte(bad))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Cells(); err == nil {
		t.Fatal("expected mismatched-zip error")
	}
}

func TestParsePlan_ModelDimensionZip_PositionPaired(t *testing.T) {
	p, err := ParsePlan([]byte(planMinimal))
	if err != nil {
		t.Fatal(err)
	}
	cells, _ := p.Cells()
	sawXAt768 := false
	sawY1536 := false
	sawXAt1536 := false
	for _, c := range cells {
		if c["model"] == "x" && reflect.DeepEqual(c["dimension"], int64(768)) {
			sawXAt768 = true
		}
		if c["model"] == "y" && reflect.DeepEqual(c["dimension"], int64(1536)) {
			sawY1536 = true
		}
		if c["model"] == "x" && reflect.DeepEqual(c["dimension"], int64(1536)) {
			sawXAt1536 = true
		}
	}
	if !sawXAt768 || !sawY1536 {
		t.Errorf("expected (x,768) and (y,1536) cells")
	}
	if sawXAt1536 {
		t.Errorf("zip violated: saw (x,1536)")
	}
}

func TestParsePlan_EmptyAxisRejected(t *testing.T) {
	bad := `
[[matrix]]
name = "endpoint"
values = []
`
	p, _ := ParsePlan([]byte(bad))
	if _, err := p.Cells(); err == nil {
		t.Fatal("expected empty-axis error")
	}
}

func TestParsePlan_NoAxesRejected(t *testing.T) {
	p, _ := ParsePlan([]byte(`sample = "demo"`))
	if _, err := p.Cells(); err == nil {
		t.Fatal("expected no-axes error")
	}
}

func TestParsePlan_SingleAxis(t *testing.T) {
	p, err := ParsePlan([]byte(`
[[matrix]]
name = "batch_size"
values = [16, 32, 64]
`))
	if err != nil {
		t.Fatal(err)
	}
	cells, err := p.Cells()
	if err != nil {
		t.Fatal(err)
	}
	if len(cells) != 3 {
		t.Fatalf("got %d cells, want 3", len(cells))
	}
}
