package config

import (
	"fmt"
	"slices"
)

func (c *RetentionConfig) Validate() error {
	if c.CleanupPolicy == "" {
		return fmt.Errorf("retention.cleanup_policy is required")
	}

	if !slices.Contains(VALID_CLEANUP_POLICIES, c.CleanupPolicy) {
		return fmt.Errorf("retention.cleanup_policy must be one of %v", VALID_CLEANUP_POLICIES)
	}

	if c.MaxAge < 0 {
		return fmt.Errorf("retention.max_age is required")
	}

	return nil
}
