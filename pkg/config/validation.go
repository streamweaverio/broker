package config

import "fmt"

// Validate StreamWeaver configuration
func (c *StreamWeaverConfig) Validate() error {
	if c.Logging == nil {
		return fmt.Errorf("logging is required")
	}

	if c.Redis == nil {
		return fmt.Errorf("redis is required")
	}

	if c.Storage == nil {
		return fmt.Errorf("storage is required")
	}

	if c.Retention == nil {
		return fmt.Errorf("retention is required")
	}

	if err := c.Logging.Validate(); err != nil {
		return err
	}

	if err := c.Redis.Validate(); err != nil {
		return err
	}

	if err := c.Storage.Validate(); err != nil {
		return err
	}

	if err := c.Retention.Validate(); err != nil {
		return err
	}

	return nil
}
