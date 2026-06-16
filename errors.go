package outbox

import "errors"

// ErrUnknownAddress is returned by AddressBook.Resolve and AddressBook.Validate
// when an address is not registered. The relay handles this as a non-retryable
// per-row condition: log + metric + leave the row for re-pickup once the
// relay learns the address (typically after a redeploy with an updated
// address book).
//
// Callers may use errors.Is to test for this condition:
//
//	if errors.Is(err, outbox.ErrUnknownAddress) { ... }
var ErrUnknownAddress = errors.New("unknown address")
