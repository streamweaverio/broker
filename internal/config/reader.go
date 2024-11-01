package config

import (
	"fmt"
	"os"

	"github.com/streamweaverio/broker/pkg/utils"
	"gopkg.in/yaml.v3"
)

// Reads the configuration file at the given path
func ReadConfiguration(filepath string) (*StreamWeaverConfig, error) {
	// set up the configuration struct with default values
	config := StreamWeaverConfig{
		Port: 3000,
		Logging: &LoggingConfig{
			LogLevel:  "INFO",
			LogOutput: "console",
			LogFormat: "text",
		},
	}

	if !utils.FileExists(filepath) {
		return nil, fmt.Errorf("config file does not exist: %s", filepath)
	}

	content, err := os.ReadFile(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %s", err)
	}

	if err := yaml.Unmarshal(content, &config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config file: %s", err)
	}

	err = config.Validate()
	if err != nil {
		return nil, fmt.Errorf("invalid configuration: %s", err)
	}

	return &config, nil
}
