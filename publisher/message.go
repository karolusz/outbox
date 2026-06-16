package publisher

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"
)

// Message is the row stored in the outbox table and the value handed to the
// Publisher. The fields it carries are everything a publisher needs to
// deliver the event to its destination, plus the bookkeeping the relay
// uses to track delivery attempts.
//
// Address is the producer-visible logical name (e.g.
// "payments.completed.v1"). The address book resolves it to a
// (publisher, target) pair at publish time.
//
// EventID is a producer-supplied UUID (UUIDv7 recommended) for end-to-end
// dedup at the broker / consumer side. If a producer leaves it empty, the
// outbox.Send helper fills it client-side; if the row is inserted via raw
// SQL with NULL or DEFAULT, the DB applies the uuidv7() default.
type Message struct {
	// Relay-managed (do not populate at insert time).
	ID              int64      `db:"id"`
	RetryCount      int        `db:"retry_count"`
	CreatedAt       time.Time  `db:"created_at"`
	LastAttemptedAt *time.Time `db:"last_attempted_at"`

	// Producer-populated (required).
	Address    string `db:"address"`
	Data       []byte `db:"data"`
	RetryLimit int    `db:"retry_limit"`

	// Producer-populated (optional — sensible defaults if zero).
	EventID     string   `db:"event_id"`
	Headers     JSONBMap `db:"headers"`
	OrderingKey string   `db:"ordering_key"`
}

// JSONBMap is a map[string]string that knows how to round-trip through a
// Postgres JSONB column.
type JSONBMap map[string]string

// Value implements driver.Valuer for writing to DB.
func (j JSONBMap) Value() (driver.Value, error) {
	if j == nil {
		return []byte("{}"), nil
	}
	return json.Marshal(j)
}

// Scan implements sql.Scanner for reading from DB.
func (j *JSONBMap) Scan(src any) error {
	if src == nil {
		*j = make(map[string]string)
		return nil
	}

	var data []byte
	switch src := src.(type) {
	case string:
		data = []byte(src)
	case []byte:
		data = src
	default:
		return fmt.Errorf("unsupported type: %T", src)
	}

	newJ := make(map[string]string)
	if err := json.Unmarshal(data, &newJ); err != nil {
		return err
	}
	*j = newJ
	return nil
}
