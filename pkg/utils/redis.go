package utils

import (
	"encoding/json"
	"fmt"
	"time"
)

// Calculates the minimum ID for a Redis stream
func CalculateRedisStreamMinID(retentionTimeInMs int64) (string, error) {
	maxAgeInSeconds := retentionTimeInMs / 1000

	duration := time.Duration(maxAgeInSeconds) * time.Second
	cutoffTimestamp := time.Now().Add(-duration).UnixMilli()
	redisTimestamp := fmt.Sprintf("%d-0", cutoffTimestamp)

	return redisTimestamp, nil
}

// Get the timestamp from a Redis stream message ID
func GetTimestampFromStreamMessageID(id string) (int64, error) {
	var timestamp int64
	_, err := fmt.Sscanf(id, "%d-0", &timestamp)
	if err != nil {
		return 0, err
	}

	return timestamp, nil
}

func SerializeStreamMessageValues(values map[string]interface{}) string {
	bytes, err := json.Marshal(values)
	if err != nil {
		return "{}"
	}

	return string(bytes)
}
