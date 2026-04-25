package bench

import (
	"errors"
	"net"
	"regexp"
	"strings"

	"github.com/wesm/msgvault/internal/vector/embed"
)

// http5xxPattern matches the status code in the embed client's
// transient-5xx error format ("embed: HTTP 5xx" or wrapped variants
// like "embed: giving up after N attempts: embed: HTTP 5xx"). The
// client emits the code as bare digits with no trailing space, so a
// substring check for "500 " misses the real-world payload.
var http5xxPattern = regexp.MustCompile(`HTTP 5\d\d`)

// PreparedMessage is the runner input — a message that has already
// been preprocessed and (if applicable) truncated. Empty Text means
// the message was dropped during preprocess and should not be sent
// to the runner.
type PreparedMessage struct {
	ID    int64
	Text  string
	Chars int
	Trunc bool
}

// EmbedClient is the subset of *embed.Client that the runners use.
// Aliased to embed.EmbeddingClient so any *embed.Client can be passed
// directly without a wrapper.
type EmbedClient = embed.EmbeddingClient

// classifyEmbedErr maps a non-nil error from EmbedClient.Embed to one
// of: "4xx", "5xx", "429", "network", "other". Used by the runners
// to bucket errors into Aggregator counters.
func classifyEmbedErr(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, embed.ErrPermanent4xx) {
		return "4xx"
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return "network"
	}
	msg := err.Error()
	if strings.Contains(msg, "429") {
		return "429"
	}
	if http5xxPattern.MatchString(msg) {
		return "5xx"
	}
	return "other"
}
