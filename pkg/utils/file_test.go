package utils

import (
	"os"
	"testing"
)

func TestFileExists(t *testing.T) {
	// Create a temporary file
	tmpFile, err := os.CreateTemp("", "testfile")
	if err != nil {
		t.Fatalf("Failed to create temporary file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	// Test case: File exists
	if !FileExists(tmpFile.Name()) {
		t.Errorf("FileExists() = false, want true")
	}

	// Test case: File does not exist
	if FileExists("/nonexistent/file/path") {
		t.Errorf("FileExists() = true, want false")
	}

	// Create a temporary directory
	tmpDir, err := os.MkdirTemp("", "testdir")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Test case: Path is a directory
	if FileExists(tmpDir) {
		t.Errorf("FileExists() = true, want false")
	}
}
