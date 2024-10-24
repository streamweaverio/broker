package config

import (
	"testing"
)

func TestLoggingConfig_Validate(t *testing.T) {
	tests := []struct {
		name          string
		config        LoggingConfig
		expectedError bool
	}{
		{
			name: "Valid console configuration",
			config: LoggingConfig{
				LogLevel:  "INFO",
				LogOutput: "console",
				LogFormat: "text",
			},
			expectedError: false,
		},
		{
			name: "Valid file configuration",
			config: LoggingConfig{
				LogLevel:      "DEBUG",
				LogOutput:     "file",
				LogFormat:     "json",
				LogFilePrefix: "app",
				LogDirectory:  "/var/log/app",
				MaxFileSize:   1048576,
			},
			expectedError: false,
		},
		{
			name: "Missing log level",
			config: LoggingConfig{
				LogOutput: "console",
				LogFormat: "text",
			},
			expectedError: true,
		},
		{
			name: "Invalid log level",
			config: LoggingConfig{
				LogLevel:  "TRACE",
				LogOutput: "console",
				LogFormat: "text",
			},
			expectedError: true,
		},
		{
			name: "Invalid log output",
			config: LoggingConfig{
				LogLevel:  "INFO",
				LogOutput: "database",
				LogFormat: "text",
			},
			expectedError: true,
		},
		{
			name: "Missing log file prefix for file output",
			config: LoggingConfig{
				LogLevel:     "INFO",
				LogOutput:    "file",
				LogFormat:    "json",
				LogDirectory: "/var/log/app",
				MaxFileSize:  1048576,
			},
			expectedError: true,
		},
		{
			name: "Invalid max file size for file output",
			config: LoggingConfig{
				LogLevel:      "INFO",
				LogOutput:     "file",
				LogFormat:     "json",
				LogFilePrefix: "app",
				LogDirectory:  "/var/log/app",
				MaxFileSize:   0,
			},
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.expectedError {
				t.Errorf("Validate() error = %v, expectedError %v", err, tt.expectedError)
			}
		})
	}
}

func TestStreamWeaverConfig_Validate(t *testing.T) {
	tests := []struct {
		name          string
		config        StreamWeaverConfig
		expectedError bool
	}{
		{
			name: "Valid configuration with logging",
			config: StreamWeaverConfig{
				Logging: &LoggingConfig{
					LogLevel:      "INFO",
					LogOutput:     "file",
					LogFormat:     "json",
					LogFilePrefix: "streamweaver",
					LogDirectory:  "/var/log/streamweaver",
					MaxFileSize:   1048576,
				},
			},
			expectedError: false,
		},
		{
			name: "Valid configuration without logging",
			config: StreamWeaverConfig{
				Logging: nil,
			},
			expectedError: false,
		},
		{
			name: "Invalid logging configuration",
			config: StreamWeaverConfig{
				Logging: &LoggingConfig{
					LogLevel:  "TRACE",
					LogOutput: "console",
					LogFormat: "text",
				},
			},
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.expectedError {
				t.Errorf("Validate() error = %v, expectedError %v", err, tt.expectedError)
			}
		})
	}
}
