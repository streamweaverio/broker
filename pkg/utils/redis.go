package utils

import (
	"fmt"
	"time"
)

// calculates the minimum ID for a Redis stream
func CalculateRedisStreamMinID(retentionTimeInMs int64) (string, error) {
	maxAgeInSeconds := retentionTimeInMs / 1000

	duration := time.Duration(maxAgeInSeconds) * time.Second
	cutoffTimestamp := time.Now().Add(-duration).UnixMilli()
	redisTimestamp := fmt.Sprintf("%d-0", cutoffTimestamp)

	return redisTimestamp, nil
}
