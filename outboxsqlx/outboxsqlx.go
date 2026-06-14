// Package outboxsqlx is a thin convenience layer for sqlx-using adopters.
//
// Importing this package lets adopters pass a native *sqlx.Tx directly to
// outbox's producer API without writing their own adapter. The package owns
// the adapter internally; everything else (SQL, args layout, retry logic)
// lives in the top-level outbox package.
package outboxsqlx

import (
	"context"

	"github.com/jmoiron/sqlx"

	"github.com/karolusz/outbox"
)

// Send saves a single Message via the given sqlx transaction. Equivalent
// to wrapping tx in an outbox.TxWriter adapter and calling outbox.Send.
func Send(ctx context.Context, tx *sqlx.Tx, msg outbox.Message) error {
	return outbox.Send(ctx, adapter{tx}, msg)
}

// SendBatch saves multiple Messages via the given sqlx transaction, in
// order. Returns immediately on the first error; the caller's tx rollback
// handles cleanup of any partial writes.
func SendBatch(ctx context.Context, tx *sqlx.Tx, msgs []outbox.Message) error {
	return outbox.SendBatch(ctx, adapter{tx}, msgs)
}

// adapter implements outbox.TxWriter by delegating to a *sqlx.Tx. The
// only thing it does is drop the unused sql.Result from ExecContext's
// return.
type adapter struct{ tx *sqlx.Tx }

func (a adapter) ExecContext(ctx context.Context, query string, args ...any) error {
	_, err := a.tx.ExecContext(ctx, query, args...)
	return err
}
