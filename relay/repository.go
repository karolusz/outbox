// A repository like methods for internal use by the outbox relay worker.
// This would be the repo layer in layered architecture but it was deemed
// unnecessary to create a separate package for just this for the outbox.
package relay

import (
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"

	"github.com/karolusz/outbox/publisher"
)

// Selector is an interface that abstracts the Select method for querying
// the database. Caller can pass *sqlx.DB or *sqlx.Tx.
type Selector interface {
	Select(dest any, query string, args ...any) error
}

// Executor is an interface that abstracts the Exec method for write
// statements. Caller can pass *sqlx.DB or *sqlx.Tx.
type Executor interface {
	Exec(query string, arg ...any) (sql.Result, error)
}

// Each repository function builds its SQL with the schema-qualified
// table name `${dbSchema}.messages`. The schema is configurable via
// relay.WithDBSchema; default "outbox" (see relay.go). For test
// convenience, an empty schema string also resolves to "outbox" — tests
// that construct &Relay{...} literals without explicitly setting
// dbSchema get the same behaviour as production.
//
// SQL is built per call via fmt.Sprintf. The cost is small (a handful
// of allocs per call, dominated by the network round-trip to Postgres)
// and the code stays straightforward.

// schemaOr returns the given schema name, or "outbox" if empty.
func schemaOr(s string) string {
	if s == "" {
		return "outbox"
	}
	return s
}

// getAllPendingEventIDs retrieves all IDs of outbox events that are
// pending processing: retry_count < retry_limit and outside the attempt
// leeway window.
func getAllPendingEventIDs(s Selector, dbSchema string, limit int, leewayDurationSec int) ([]int64, error) {
	eventIDs := []int64{}
	query := fmt.Sprintf(`
		SELECT id
		FROM %s.messages
		WHERE retry_count < retry_limit
		  AND ((last_attempted_at IS NULL) OR (last_attempted_at <= NOW() - ($2 * INTERVAL '1 second')))
		LIMIT $1
	`, schemaOr(dbSchema))
	err := s.Select(&eventIDs, query, limit, leewayDurationSec)
	return eventIDs, err
}

// getEventByIDIfNotLocked retrieves a single outbox event by its ID
// within the provided transaction. SELECTs explicit columns rather than
// *, so the presence of additional adopter-side columns does not break
// the scan.
func getEventByIDIfNotLocked(tx *sqlx.Tx, dbSchema string, id int64) (*publisher.Message, error) {
	var event publisher.Message
	query := fmt.Sprintf(`
		SELECT id, event_id, address, data, headers, ordering_key,
		       retry_count, retry_limit, created_at, last_attempted_at
		FROM %s.messages
		WHERE id = $1 AND retry_count < retry_limit
		FOR NO KEY UPDATE OF messages SKIP LOCKED
	`, schemaOr(dbSchema))
	err := tx.Get(&event, query, id)
	if err != nil {
		return nil, err
	}
	return &event, nil
}

// deleteOneEvent deletes a single outbox event by its ID within the
// provided transaction.
func deleteOneEvent(ex Executor, dbSchema string, id int64) error {
	query := fmt.Sprintf(`DELETE FROM %s.messages WHERE id=$1`, schemaOr(dbSchema))
	_, err := ex.Exec(query, id)
	return err
}

// incrementRetryCount increments the retry count and updates the
// last_attempted_at timestamp for the given outbox event ID within the
// provided transaction.
func incrementRetryCount(tx *sqlx.Tx, dbSchema string, id int64) error {
	query := fmt.Sprintf(`
		UPDATE %s.messages
		SET retry_count = retry_count + 1,
		    last_attempted_at = NOW()
		WHERE id = $1
	`, schemaOr(dbSchema))
	_, err := tx.Exec(query, id)
	return err
}

// setLastAttemptedAt updates last_attempted_at to NOW() WITHOUT touching
// retry_count. Used by the relay when it picks up a row whose address is
// not registered in the address book: the row stays available
// (retry_count is preserved, so it never hits the limit and never gets
// filtered out of polling), but is throttled out of the worker's next
// pass by the leeway window. Once the relay redeploys with a complete
// address book, the row publishes normally.
//
// CRITICAL invariant: this MUST NOT increment retry_count. If it did,
// unknown-address rows would eventually hit retry_limit and become
// invisible to polling — silent data loss exactly when an operator
// would want to recover them by adding the missing address.
func setLastAttemptedAt(tx *sqlx.Tx, dbSchema string, id int64) error {
	query := fmt.Sprintf(`UPDATE %s.messages SET last_attempted_at = NOW() WHERE id = $1`, schemaOr(dbSchema))
	_, err := tx.Exec(query, id)
	return err
}

// tryIncrementRetryCount increments retry_count and last_attempted_at
// for the row, but only if both:
//   - the row is NOT currently locked by another transaction, AND
//   - retry_count is still below retry_limit.
//
// Returns:
//   - (true, nil)  → the increment was applied
//   - (false, nil) → skipped (row locked by another worker, OR already at
//     retry_limit — the cap matches the poll query's WHERE clause so we
//     don't push a row past the limit through the panic-marking path)
//   - (false, err) → query error
//
// Implementation: a CTE filters on (row not locked) AND (retry_count <
// retry_limit) using `FOR NO KEY UPDATE SKIP LOCKED`. If either
// predicate rules the row out, the CTE returns empty and the UPDATE
// affects zero rows — without blocking.
func tryIncrementRetryCount(tx *sqlx.Tx, dbSchema string, id int64) (bool, error) {
	var returnedID int64
	query := fmt.Sprintf(`
		WITH locked AS (
			SELECT id FROM %s.messages
			WHERE id = $1 AND retry_count < retry_limit
			FOR NO KEY UPDATE SKIP LOCKED
		)
		UPDATE %s.messages
		SET retry_count = retry_count + 1,
		    last_attempted_at = NOW()
		WHERE id IN (SELECT id FROM locked)
		RETURNING id
	`, schemaOr(dbSchema), schemaOr(dbSchema))
	err := tx.Get(&returnedID, query, id)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}
