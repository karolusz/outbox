package publisher

import (
	"crypto/rand"
	"encoding/hex"
	"time"
)

// NewEventID returns a fresh UUIDv7 (RFC 9562) as a canonical
// 36-character string. The high 48 bits are the Unix timestamp in
// milliseconds; the remaining bits are random (with the spec-mandated
// version and variant nibbles set).
//
// UUIDv7 is time-ordered, giving better B-tree index locality than v4
// and a natural sort order that aligns with insertion time. We use it
// as the recommended event-id format. The DB column has a uuidv7()
// DEFAULT for adopters who don't supply one; outbox.Send fills empty
// EventIDs via this helper so the producer can log/trace the ID before
// the INSERT executes.
//
// Implementation is stdlib-only (crypto/rand + time). Keeping the
// publisher package free of third-party deps preserves the producer's
// "stdlib only" go.sum invariant — see make check-producer-deps.
func NewEventID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand failing is system-level catastrophic; nothing else
		// in the program can sensibly continue. Panic surfaces it.
		panic("publisher: crypto/rand failure: " + err.Error())
	}

	// Write the 48-bit Unix-milliseconds timestamp into the first 6
	// bytes, big-endian.
	ms := uint64(time.Now().UnixMilli())
	b[0] = byte(ms >> 40)
	b[1] = byte(ms >> 32)
	b[2] = byte(ms >> 24)
	b[3] = byte(ms >> 16)
	b[4] = byte(ms >> 8)
	b[5] = byte(ms)

	// Version nibble: top 4 bits of byte 6 become 0b0111 (= 7).
	b[6] = (b[6] & 0x0F) | 0x70
	// Variant nibble: top 2 bits of byte 8 become 0b10.
	b[8] = (b[8] & 0x3F) | 0x80

	// Canonical 8-4-4-4-12 hex form. 36 chars total (32 hex + 4 dashes).
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
