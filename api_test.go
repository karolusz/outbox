//go:build testing

package outbox

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestSendMessage_CanCreateDBEntry ensures that SendMessage can create a database entry without error.
func TestSendMessage_CanCreateDBEntry(t *testing.T) {
	db, _ := setupTest(t, "TestSendMessage_CanCreateDBEntry", "")
	event := Message{}
	err := SendMessage(db, event)
	assert.NoError(t, err, "SEndMessage should not return an error")
}
