package config

import (
	"fmt"
	"slices"
	"strings"
)

// Validate logging configuration
func (c *LoggingConfig) Validate() error {
	if c.LogLevel == "" {
		return fmt.Errorf("logging.log_level is required")
	}

	if !slices.Contains(VALID_LOG_LEVELS, strings.ToUpper(c.LogLevel)) {
		return fmt.Errorf("logging.log_level must be one of: %v", VALID_LOG_LEVELS)
	}

	if c.LogOutput == "" {
		return fmt.Errorf("logging.log_output is required")
	}

	if !slices.Contains(VALID_LOG_OUTPUTS, c.LogOutput) {
		return fmt.Errorf("logging.log_output must be one of: %v", VALID_LOG_OUTPUTS)
	}

	if c.LogFormat == "" {
		return fmt.Errorf("logging.log_format is required")
	}

	if !slices.Contains(VALID_LOG_FORMATS, c.LogFormat) {
		return fmt.Errorf("logging.log_format must be one of: %v", VALID_LOG_FORMATS)
	}

	if c.LogOutput == "file" {
		if c.LogFilePrefix == "" {
			return fmt.Errorf("logging.log_file_prefix is required when logging.log_output is 'file'")
		}

		if c.LogDirectory == "" {
			return fmt.Errorf("logging.log_directory is required when logging.log_output is 'file'")
		}

		if c.MaxFileSize <= 0 {
			return fmt.Errorf("logging.max_file_size must be greater than 0")
		}
	}

	return nil
}
