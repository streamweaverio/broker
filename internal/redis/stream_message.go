package redis

import (
	"bytes"
	"fmt"
)

// Converts a slice of byte slices into a slice of maps that can be used with Redis.
func ByteSliceToRedisMessageMapSlice(values [][]byte) []map[string]interface{} {
	result := make([]map[string]interface{}, len(values))
	for i, v := range values {
		// Parse the space-separated key-value pairs from the message_content
		messageMap := make(map[string]interface{})
		pairs := bytes.Split(v, []byte(" "))
		for _, pair := range pairs {
			keyValue := bytes.SplitN(pair, []byte("="), 2)
			if len(keyValue) == 2 {
				key := string(keyValue[0])
				value := string(keyValue[1])
				messageMap[key] = value
			} else {
				// Handle malformed entries
				messageMap[fmt.Sprintf("malformed_%d", i)] = string(pair)
			}
		}

		// Add the parsed message map to the result slice
		result[i] = messageMap
	}
	return result
}
