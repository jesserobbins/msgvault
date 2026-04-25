package bench

import (
	"bytes"
	"fmt"

	"github.com/BurntSushi/toml"
)

// Plan is a parsed sweep plan: fixed parameters + a matrix of axes.
type Plan struct {
	Notes         string         `toml:"notes"`
	Sample        string         `toml:"sample"`
	Mode          string         `toml:"mode"` // "endpoint" | "pipeline"
	WarmupBatches int            `toml:"warmup_batches"`
	Fixed         map[string]any `toml:"fixed"`
	Matrix        []MatrixAxis   `toml:"matrix"`
}

// MatrixAxis declares one matrix dimension. Values are []any so the
// TOML scalar type (string/int/float) is preserved through expansion.
type MatrixAxis struct {
	Name   string `toml:"name"`
	Values []any  `toml:"values"`
}

// Cell is one matrix cell: a map from axis name to scalar value.
type Cell map[string]any

// ParsePlan parses TOML bytes into a Plan and applies defaults
// (mode="endpoint", warmup_batches=1).
func ParsePlan(data []byte) (*Plan, error) {
	var p Plan
	if _, err := toml.NewDecoder(bytes.NewReader(data)).Decode(&p); err != nil {
		return nil, fmt.Errorf("ParsePlan: %w", err)
	}
	if p.Mode == "" {
		p.Mode = "endpoint"
	}
	if p.WarmupBatches == 0 {
		p.WarmupBatches = 1
	}
	return &p, nil
}

// Cells expands the matrix into the cartesian product of axes, with
// the special case that "model" and "dimension" axes (when both
// declared) are zipped position-wise rather than crossed. The two
// must have equal length when both present.
func (p *Plan) Cells() ([]Cell, error) {
	if len(p.Matrix) == 0 {
		return nil, fmt.Errorf("plan has no matrix axes")
	}
	for _, ax := range p.Matrix {
		if len(ax.Values) == 0 {
			return nil, fmt.Errorf("matrix axis %q has no values", ax.Name)
		}
	}

	type fusedPair struct {
		Model string
		Dim   int64
	}
	var fused []fusedPair
	modelIdx, dimIdx := -1, -1
	for i, ax := range p.Matrix {
		if ax.Name == "model" {
			modelIdx = i
		}
		if ax.Name == "dimension" {
			dimIdx = i
		}
	}
	if modelIdx >= 0 && dimIdx >= 0 {
		ms, ds := p.Matrix[modelIdx].Values, p.Matrix[dimIdx].Values
		if len(ms) != len(ds) {
			return nil, fmt.Errorf("matrix axes 'model' and 'dimension' must have equal length when both present (got %d and %d)", len(ms), len(ds))
		}
		for i := range ms {
			msStr, ok := ms[i].(string)
			if !ok {
				return nil, fmt.Errorf("matrix axis 'model' value %d is not a string: %v", i, ms[i])
			}
			d, err := toInt64(ds[i])
			if err != nil {
				return nil, fmt.Errorf("matrix axis 'dimension' value %d: %w", i, err)
			}
			fused = append(fused, fusedPair{Model: msStr, Dim: d})
		}
	}

	type axis struct {
		Name   string
		Values []any
	}
	var axes []axis
	for i, ax := range p.Matrix {
		if i == modelIdx || i == dimIdx {
			continue
		}
		axes = append(axes, axis{Name: ax.Name, Values: ax.Values})
	}
	if len(fused) > 0 {
		anyVals := make([]any, len(fused))
		for i, fp := range fused {
			anyVals[i] = fp
		}
		axes = append(axes, axis{Name: "__model_dim__", Values: anyVals})
	}

	var out []Cell
	var rec func(i int, cur Cell)
	rec = func(i int, cur Cell) {
		if i == len(axes) {
			cell := make(Cell, len(cur)+1)
			for k, v := range cur {
				if k == "__model_dim__" {
					fp := v.(fusedPair)
					cell["model"] = fp.Model
					cell["dimension"] = fp.Dim
				} else {
					cell[k] = v
				}
			}
			out = append(out, cell)
			return
		}
		for _, v := range axes[i].Values {
			cur[axes[i].Name] = v
			rec(i+1, cur)
			delete(cur, axes[i].Name)
		}
	}
	rec(0, Cell{})
	return out, nil
}

// toInt64 normalizes TOML's int representations. The BurntSushi
// decoder produces int64 for TOML integers, but this helper is
// defensive about float64 (rare TOML edge case) and plain int.
func toInt64(v any) (int64, error) {
	switch x := v.(type) {
	case int64:
		return x, nil
	case int:
		return int64(x), nil
	case float64:
		return int64(x), nil
	default:
		return 0, fmt.Errorf("not an int: %T %v", v, v)
	}
}
