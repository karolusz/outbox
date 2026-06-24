package publisher

import (
	"crypto/rand"
	"encoding/hex"
	"time"
)

// NewEventID returns a fresh UUIDv7 (RFC 9562) as a canonical
// 36-character string. UUIDv7 is time-ordered for better B-tree index
// locality than v4 and a natural sort that aligns with insertion time.
// The DB column has a uuidv7() default; outbox.Send fills empty
// EventIDs via this helper so producers can log the ID before INSERT.
func NewEventID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("publisher: crypto/rand failure: " + err.Error())
	}

	ms := uint64(time.Now().UnixMilli())
	b[0] = byte(ms >> 40)
	b[1] = byte(ms >> 32)
	b[2] = byte(ms >> 24)
	b[3] = byte(ms >> 16)
	b[4] = byte(ms >> 8)
	b[5] = byte(ms)

	b[6] = (b[6] & 0x0F) | 0x70 // version 7
	b[8] = (b[8] & 0x3F) | 0x80 // variant 10

	var out [36]byte
	hex.Encode(out[0:8], b[0:4])
	out[8] = '-'
	hex.Encode(out[9:13], b[4:6])
	out[13] = '-'
	hex.Encode(out[14:18], b[6:8])
	out[18] = '-'
	hex.Encode(out[19:23], b[8:10])
	out[23] = '-'
	hex.Encode(out[24:36], b[10:16])
	return string(out[:])
}
