package utils

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

func IsValidTimeUnitString(value string) bool {
	pattern := regexp.MustCompile(`^(\d+)(d|h)$`)
	matches := pattern.FindStringSubmatch(value)

	return matches != nil
}

// Parses a time unit string into a duration in seconds
func ParseTimeUnitString(value string) (int, error) {
	pattern := regexp.MustCompile(`^(\d+)(d|h)$`)
	matches := pattern.FindStringSubmatch(value)

	if matches == nil {
		return 0, fmt.Errorf("invalid time unit string: %s", value)
	}

	parsedValue, err := strconv.ParseInt(matches[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse time unit value. invalid value in string: %s", matches[1])
	}

	unit := matches[2]
	var duration int

	switch strings.ToLower(unit) {
	case "d":
		duration = int(parsedValue) * 86400
	case "h":
		duration = int(parsedValue) * 3600
	default:
		return 0, fmt.Errorf("invalid time unit: %s. Use d for days, h for hours", unit)
	}

	return duration, nil
}
