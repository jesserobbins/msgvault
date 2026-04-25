package bench

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// MarshalCanonical produces a JSON encoding with object keys
// recursively sorted alphabetically. Relies on encoding/json's
// documented behavior (Go 1.12+) of sorting map keys at every nesting
// level. The canonicalize walk rebuilds nested values as map[string]any
// / []any so any non-map wrapper types are recursed into uniformly.
// Slice order is preserved.
func MarshalCanonical(v any) ([]byte, error) {
	return json.Marshal(canonicalize(v))
}

// ConfigHash returns the hex-encoded sha256 of the canonical encoding.
// Two values that differ only in map key order produce the same hash.
func ConfigHash(v any) (string, error) {
	b, err := MarshalCanonical(v)
	if err != nil {
		return "", fmt.Errorf("bench: canonical marshal: %w", err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// canonicalize recursively normalizes v into map[string]any / []any
// wrappers. For pure map[string]any inputs this is a no-op relative to
// json.Marshal's own key sorting, but it ensures correctness if callers
// pass typed maps or other wrapper types that json.Marshal would sort
// differently. Other types pass through unchanged.
func canonicalize(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[k] = canonicalize(val)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, x := range t {
			out[i] = canonicalize(x)
		}
		return out
	default:
		return t
	}
}
