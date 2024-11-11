package config

import (
	"fmt"
)

func (c *RetentionConfig) Validate() error {
	if c.MaxAge < 0 {
		return fmt.Errorf("retention.max_age is required")
	}

	return nil
}
