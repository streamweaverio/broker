package config

import (
	"fmt"
	"slices"
)

func (c *AWSS3StorageProviderConfig) Validate() error {
	if c.AccessKeyId == "" {
		return fmt.Errorf("storage.aws_s3.access_key_id is required")
	}

	if c.SecretAccessKey == "" {
		return fmt.Errorf("storage.aws_s3.secret_access_key is required")
	}

	if c.Region == "" {
		return fmt.Errorf("storage.aws_s3.region is required")
	}

	if c.Bucket == "" {
		return fmt.Errorf("storage.aws_s3.bucket is required")
	}

	return nil
}

func (c *StorageConfig) Validate() error {
	if c.Provider == "" {
		return fmt.Errorf("storage.provider is required")
	}

	if !slices.Contains(VALID_STORAGE_PROVIDERS, c.Provider) {
		return fmt.Errorf("storage.provider must be one of: %v", VALID_STORAGE_PROVIDERS)
	}

	if c.Provider == "local" {
		if c.Local == nil {
			return fmt.Errorf("storage.local is required when storage.provider is set to 'local'")
		}

		if c.Local.Directory == "" {
			return fmt.Errorf("storage.local.directory is required when storage.provider is set to 'local'")
		}
	}

	if c.Provider == "s3" {
		if c.S3 == nil {
			return fmt.Errorf("storage.aws_s3 is required when storage.provider is set to 's3'")
		}

		return c.S3.Validate()
	}

	return nil
}
