package config

import (
	"fmt"

	"github.com/streamweaverio/broker/pkg/utils"
)

func (c *RetentionConfig) Validate() error {
	if c.MaxAge == "" {
		return fmt.Errorf("retention.max_age is required")
	}

	if !utils.IsValidTimeUnitString(c.MaxAge) {
		return fmt.Errorf("retention.max_age must be a valid time unit string")
	}

	return nil
}
