package config

import "testing"

type LoggingConfigTestCase struct {
	Name        string        `json:"name"`
	Value       LoggingConfig `json:"config"`
	ExpectError bool          `json:"expectedError"`
}

func TestLoggingConfig_Validate(t *testing.T) {
	cases := []LoggingConfigTestCase{
		{
			Name: "Valid console configuration",
			Value: LoggingConfig{
				LogLevel:  "INFO",
				LogOutput: "console",
				LogFormat: "text",
			},
			ExpectError: false,
		},
		{
			Name: "Valid file configuration",
			Value: LoggingConfig{
				LogLevel:      "DEBUG",
				LogOutput:     "file",
				LogFormat:     "json",
				LogFilePrefix: "app",
				LogDirectory:  "/var/log/app",
				MaxFileSize:   1048576,
			},
			ExpectError: false,
		},
		{
			Name: "Missing log level",
			Value: LoggingConfig{
				LogOutput: "console",
				LogFormat: "text",
			},
			ExpectError: true,
		},
		{
			Name: "Invalid log level",
			Value: LoggingConfig{
				LogLevel:  "TRACE",
				LogOutput: "console",
				LogFormat: "text",
			},
			ExpectError: true,
		},
		{
			Name: "Invalid log output",
			Value: LoggingConfig{
				LogLevel:  "INFO",
				LogOutput: "database",
				LogFormat: "text",
			},
			ExpectError: true,
		},
		{
			Name: "Missing log output",
			Value: LoggingConfig{
				LogLevel:  "INFO",
				LogFormat: "text",
			},
			ExpectError: true,
		},
		{
			Name: "Missing log format",
			Value: LoggingConfig{
				LogLevel:  "INFO",
				LogOutput: "console",
			},
			ExpectError: true,
		},
		{
			Name: "Invalid log format",
			Value: LoggingConfig{
				LogLevel:  "INFO",
				LogOutput: "console",
				LogFormat: "xml",
			},
			ExpectError: true,
		},
		{
			Name: "Missing log file prefix for file output",
			Value: LoggingConfig{
				LogLevel:     "INFO",
				LogOutput:    "file",
				LogFormat:    "json",
				LogDirectory: "/var/log/app",
				MaxFileSize:  1048576,
			},
			ExpectError: true,
		},
		{
			Name: "Missing log directory for file output",
			Value: LoggingConfig{
				LogLevel:      "INFO",
				LogOutput:     "file",
				LogFormat:     "json",
				LogFilePrefix: "app",
				MaxFileSize:   1048576,
			},
			ExpectError: true,
		},
		{
			Name: "Invalid max file size",
			Value: LoggingConfig{
				LogLevel:      "INFO",
				LogOutput:     "file",
				LogFormat:     "json",
				LogFilePrefix: "app",
				LogDirectory:  "/var/log/app",
				MaxFileSize:   0,
			},
			ExpectError: true,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.Name, func(t *testing.T) {
			err := testCase.Value.Validate()
			if (err != nil) != testCase.ExpectError {
				t.Errorf("Validate() error = %v, expectedError %v", err, testCase.ExpectError)
			}
		})
	}
}
