package publisher

import (
	"context"
	"time"
)

// WithOptionalTimeout derives a child context with a deadline d in the
// future, OR returns the parent ctx unchanged if d <= 0. Always returns
// a non-nil cancel function (a no-op when d <= 0), so callers can
// unconditionally `defer cancel()`.
//
// Helper for plugins and the relay that want to apply a ctx deadline
// only when one is configured. Avoids the standard `var cancel
// context.CancelFunc; ctx, cancel = context.WithTimeout(...)` shadowing
// dance — `:=` inside an `if` block introduces a new ctx scoped to the
// block, silently dropping the timeout wrapping.
//
// stdlib-only; safe to use from any package without breaking the
// producer-side packages' "no third-party deps" invariant.
func WithOptionalTimeout(parent context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if d <= 0 {
		return parent, func() {}
	}
	return context.WithTimeout(parent, d)
}
