package outbox

import "github.com/karolusz/outbox/publisher"

// Message is the row stored in the outbox table and the value handed to
// the Publisher. Re-exported here as a type alias so producer code stays
// outbox.Message{...} without importing the publisher package directly.
// The canonical definition lives in github.com/karolusz/outbox/publisher.
type Message = publisher.Message
