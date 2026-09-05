package digest

import (
	"crypto/sha256"
	"encoding/hex"
)

func ContentHash(content []byte) string {
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:])
}
