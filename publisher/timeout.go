package publisher

import (
	"context"
	"time"
)

// WithOptionalTimeout derives a child context with a d-future deadline,
// or returns parent unchanged if d <= 0. Always returns a non-nil cancel
// (no-op when d <= 0) so callers can unconditionally `defer cancel()`.
func WithOptionalTimeout(parent context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if d <= 0 {
		return parent, func() {}
	}
	return context.WithTimeout(parent, d)
}
