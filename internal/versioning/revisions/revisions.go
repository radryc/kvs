package revisions

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

func NewVersionID() string { return newID("ver") }

func NewBatchRevisionID() string { return newID("batch") }

func NewCorrelationID() string { return newID("corr") }

func newID(prefix string) string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%s_%d", prefix, time.Now().UTC().UnixNano())
	}
	return fmt.Sprintf("%s_%d_%s", prefix, time.Now().UTC().UnixNano(), hex.EncodeToString(b[:]))
}
