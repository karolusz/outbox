package fake

import (
	"context"

	"github.com/karolusz/outbox"
)

// Config is the YAML-visible configuration for the fake publisher.
//
// There is nothing to configure — the fake publisher records messages in
// memory and exposes channels / counters for tests. The type exists so
// the YAML factory has a target to unmarshal into, and for symmetry with
// other plugins.
type Config struct{}

// init registers the fake plugin with the outbox registry. Adopters
// trigger this side effect via:
//
//	import _ "github.com/karolusz/outbox/publisher/fake"
//
// Useful in test binaries and for "soft launch" setups where the YAML
// references plugin "fake" instead of a real broker.
func init() {
	outbox.RegisterPlugin("fake", func(ctx context.Context, decode outbox.ConfigDecoder) (outbox.Publisher, error) {
		// The fake publisher takes no config; the decoder is not invoked.
		return New(), nil
	})
}
