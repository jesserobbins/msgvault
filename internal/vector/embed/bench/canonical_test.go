package bench

import "testing"

func TestConfigHash_OrderIndependent(t *testing.T) {
	a := map[string]any{"endpoint": "http://x", "model": "nomic", "batch_size": 32}
	b := map[string]any{"model": "nomic", "batch_size": 32, "endpoint": "http://x"}
	ha, err := ConfigHash(a)
	if err != nil {
		t.Fatal(err)
	}
	hb, err := ConfigHash(b)
	if err != nil {
		t.Fatal(err)
	}
	if ha != hb {
		t.Fatalf("ConfigHash differs: %s vs %s", ha, hb)
	}
}

func TestConfigHash_Deterministic(t *testing.T) {
	m := map[string]any{"endpoint": "http://x", "n": 1, "nested": map[string]any{"b": 2, "a": 1}}
	h1, err := ConfigHash(m)
	if err != nil {
		t.Fatal(err)
	}
	h2, err := ConfigHash(m)
	if err != nil {
		t.Fatal(err)
	}
	if h1 != h2 {
		t.Fatalf("not deterministic: %s vs %s", h1, h2)
	}
}

func TestConfigHash_NestedOrderIndependent(t *testing.T) {
	a := map[string]any{"x": map[string]any{"a": 1, "b": 2}}
	b := map[string]any{"x": map[string]any{"b": 2, "a": 1}}
	ha, _ := ConfigHash(a)
	hb, _ := ConfigHash(b)
	if ha != hb {
		t.Fatalf("nested order not normalized: %s vs %s", ha, hb)
	}
}
