package config

import "fmt"

func (c *RedisConfig) Validate() error {
	if len(c.Hosts) < 1 {
		return fmt.Errorf("redis.hosts is required")
	}

	for index, host := range c.Hosts {
		if host.Host == "" {
			return fmt.Errorf("redis.hosts[%d].host is required", index)
		}

		if host.Port <= 0 {
			return fmt.Errorf("redis.hosts[%d].port must be greater than 0", index)
		}
	}

	if c.DB < 0 {
		return fmt.Errorf("redis.db must be greater than or equal to 0")
	}

	return nil
}
