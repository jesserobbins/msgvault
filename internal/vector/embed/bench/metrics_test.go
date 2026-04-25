package bench

import (
	"math"
	"testing"
	"time"
)

func TestPercentile_KnownDistribution(t *testing.T) {
	// 100 samples 50..149 ms (uniform).
	var ds []time.Duration
	for i := range 100 {
		ds = append(ds, time.Duration(50+i)*time.Millisecond)
	}
	p50 := PercentileMs(ds, 50)
	if math.Abs(p50-99.5) > 1.0 {
		t.Errorf("p50: got %.2f, want ~99.5", p50)
	}
	p95 := PercentileMs(ds, 95)
	if math.Abs(p95-144.5) > 1.0 {
		t.Errorf("p95: got %.2f, want ~144.5", p95)
	}
	pMax := MaxMs(ds)
	if math.Abs(pMax-149.0) > 0.001 {
		t.Errorf("max: got %.2f, want 149.0", pMax)
	}
}

func TestPercentile_Empty(t *testing.T) {
	if !math.IsNaN(PercentileMs(nil, 50)) {
		t.Errorf("empty input should yield NaN")
	}
	if !math.IsNaN(MaxMs(nil)) {
		t.Errorf("MaxMs empty should yield NaN")
	}
}

func TestPercentile_Single(t *testing.T) {
	ds := []time.Duration{42 * time.Millisecond}
	if math.Abs(PercentileMs(ds, 50)-42) > 0.001 {
		t.Errorf("single-sample p50: got %.2f", PercentileMs(ds, 50))
	}
	if math.Abs(PercentileMs(ds, 99)-42) > 0.001 {
		t.Errorf("single-sample p99: got %.2f", PercentileMs(ds, 99))
	}
}

func TestPercentile_Bounds(t *testing.T) {
	ds := []time.Duration{
		10 * time.Millisecond,
		20 * time.Millisecond,
		30 * time.Millisecond,
	}
	if math.Abs(PercentileMs(ds, 0)-10) > 0.001 {
		t.Errorf("p0 should be min: got %.2f", PercentileMs(ds, 0))
	}
	if math.Abs(PercentileMs(ds, 100)-30) > 0.001 {
		t.Errorf("p100 should be max: got %.2f", PercentileMs(ds, 100))
	}
}

func TestAggregator_NoNaNOnEmpty(t *testing.T) {
	a := NewAggregator()
	a.Start()
	a.Stop()
	r := a.Result()
	if r.MsgPerSec != 0 {
		t.Errorf("MsgPerSec on empty: got %v, want 0", r.MsgPerSec)
	}
	if r.UsPerChar != 0 {
		t.Errorf("UsPerChar on empty: got %v, want 0", r.UsPerChar)
	}
	if r.MsPerMsg != 0 {
		t.Errorf("MsPerMsg on empty: got %v, want 0", r.MsPerMsg)
	}
	if !math.IsNaN(r.BatchP50) {
		t.Errorf("BatchP50 on empty: got %v, want NaN", r.BatchP50)
	}
	if !math.IsNaN(r.BatchMax) {
		t.Errorf("BatchMax on empty: got %v, want NaN", r.BatchMax)
	}
}

func TestAggregator_AddBatch(t *testing.T) {
	a := NewAggregator()
	a.Start()
	a.AddBatch(10, 1000, 100*time.Millisecond, 80*time.Millisecond, 5*time.Millisecond, 10*time.Millisecond, 5*time.Millisecond, 2)
	a.AddBatch(20, 2000, 200*time.Millisecond, 160*time.Millisecond, 10*time.Millisecond, 20*time.Millisecond, 10*time.Millisecond, 0)
	a.Stop()
	r := a.Result()
	if r.Msgs != 30 {
		t.Errorf("Msgs: got %d, want 30", r.Msgs)
	}
	if r.Chars != 3000 {
		t.Errorf("Chars: got %d, want 3000", r.Chars)
	}
	if r.Truncated != 2 {
		t.Errorf("Truncated: got %d, want 2", r.Truncated)
	}
	if r.EmbedMsSum != 240 {
		t.Errorf("EmbedMsSum: got %d, want 240", r.EmbedMsSum)
	}
	// 30 msgs / 0.240 sec = 125 msgs/sec
	if math.Abs(r.MsgPerSec-125.0) > 0.5 {
		t.Errorf("MsgPerSec: got %.2f, want ~125", r.MsgPerSec)
	}
	// 240 ms / 30 msgs = 8 ms/msg
	if math.Abs(r.MsPerMsg-8.0) > 0.5 {
		t.Errorf("MsPerMsg: got %.2f, want ~8.0", r.MsPerMsg)
	}
	// 240_000 us / 3000 chars = 80 us/char
	if math.Abs(r.UsPerChar-80.0) > 1.0 {
		t.Errorf("UsPerChar: got %.2f, want ~80", r.UsPerChar)
	}
	if r.ClaimMs != 15 {
		t.Errorf("ClaimMs: got %d, want 15", r.ClaimMs)
	}
	if r.UpsertMs != 30 {
		t.Errorf("UpsertMs: got %d, want 30", r.UpsertMs)
	}
	if r.CompleteMs != 15 {
		t.Errorf("CompleteMs: got %d, want 15", r.CompleteMs)
	}
}

func TestAggregator_AddError(t *testing.T) {
	a := NewAggregator()
	a.AddError("4xx")
	a.AddError("4xx")
	a.AddError("5xx")
	a.AddError("429")
	a.AddError("network")
	a.AddError("other") // unknown class — silently ignored
	r := a.Result()
	if r.Errors4xx != 2 {
		t.Errorf("Errors4xx: got %d, want 2", r.Errors4xx)
	}
	if r.Errors5xx != 1 {
		t.Errorf("Errors5xx: got %d, want 1", r.Errors5xx)
	}
	if r.Errors429 != 1 {
		t.Errorf("Errors429: got %d, want 1", r.Errors429)
	}
	if r.ErrorsNet != 1 {
		t.Errorf("ErrorsNet: got %d, want 1", r.ErrorsNet)
	}
}

func TestAggregator_AddRetries(t *testing.T) {
	a := NewAggregator()
	a.AddRetries(3)
	a.AddRetries(2)
	if r := a.Result(); r.Retries != 5 {
		t.Errorf("Retries: got %d, want 5", r.Retries)
	}
}

func TestAggregator_ElapsedMs(t *testing.T) {
	a := NewAggregator()
	a.Start()
	time.Sleep(10 * time.Millisecond)
	a.Stop()
	r := a.Result()
	if r.ElapsedMs < 9 {
		t.Errorf("ElapsedMs: got %d, want >=9", r.ElapsedMs)
	}
}
