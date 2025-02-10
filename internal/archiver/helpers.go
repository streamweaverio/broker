package archiver

import (
	"crypto/sha256"
	"fmt"
)

// Generates a unique block ID based on the start and end timestamps
func GenerateBlockID(timestampStart string, timestampEnd string) string {
	blockRange := fmt.Sprintf("%s-%s", timestampStart, timestampEnd)
	return fmt.Sprintf("block-%x", sha256.Sum256([]byte(blockRange)))
}
