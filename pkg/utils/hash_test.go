package utils

import (
	"testing"
)

func TestHashString(t *testing.T) {
	tests := []struct {
		input    string
		expected uint64
	}{
		{"", 0},                // Empty string
		{"a", 97},              // Single character
		{"abc", 108966},        // Update with actual value
		{"hello", 127086708},   // Update with actual value
		{"GoLang", 2913090712}, // Update with actual value
		{"This is a test string", 5789433518235566252}, // Update with actual value
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := HashString(tt.input)
			if result != tt.expected {
				t.Errorf("HashString(%q) = %v; want %v", tt.input, result, tt.expected)
			}
		})
	}
}
