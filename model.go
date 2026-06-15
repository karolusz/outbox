package outbox

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"
)

// Message is the row stored in the outbox table and the value handed to the
// Publisher. The fields it carries are everything a publisher needs to
// deliver the event to its destination, plus the bookkeeping the relay uses
// to track delivery attempts.
//
// Address is the producer-visible logical name (e.g. "payments.completed.v1").
// In v0.2+, the address book resolves it to a (publisher, target) pair at
// publish time. In v0.1 setups without an address book, the address is
// passed through as the broker target verbatim. The db column remains
// "topic" via struct tag for backwards compatibility with the v0.1 schema.
type Message struct {
	ID              int64      `db:"id"`
	Data            []byte     `db:"data"`
	Attributes      JSONBMap   `db:"attributes"`
	Address         string     `db:"topic"` // logical address; db column kept as "topic"
	OrderingKey     string     `db:"ordering_key"`
	EventType       string     `db:"event_type"`
	RetryCount      int        `db:"retry_count"`
	RetryLimit      int        `db:"retry_limit"`
	CreatedAt       time.Time  `db:"created_at"`
	LastAttemptedAt *time.Time `db:"last_attempted_at"`
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
