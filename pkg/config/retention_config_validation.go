package config

import (
	"fmt"
	"slices"
)

func (c *RetentionConfig) Validate() error {
	if c.Policy == "" {
		return fmt.Errorf("retention.policy is required")
	}

	if !slices.Contains(VALID_RETENTION_POLICIES, c.Policy) {
		return fmt.Errorf("retention.policy must be one of: %v", VALID_RETENTION_POLICIES)
	}

	if c.Policy == "time" {
		if c.MaxAge == "" {
			return fmt.Errorf("retention.max_age is required when retention.policy is 'time'")
		}
	}

	if c.Policy == "size" {
		if c.MaxSize <= 0 {
			return fmt.Errorf("retention.max_size must be greater than 0 when retention.policy is 'size'")
		}
	}

	return nil
}
