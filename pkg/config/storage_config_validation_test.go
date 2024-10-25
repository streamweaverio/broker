package config

import "testing"

type StorageConfigTestCase struct {
	Name        string        `json:"name"`
	Value       StorageConfig `json:"config"`
	ExpectError bool          `json:"expectedError"`
}

func TestStorageConfig_Validate(t *testing.T) {
	testCases := []StorageConfigTestCase{
		{
			Name: "Invalid storage provider",
			Value: StorageConfig{
				Provider: "nfs",
			},
			ExpectError: true,
		},
		{
			Name: "Invalid local configuration",
			Value: StorageConfig{
				Provider: "local",
			},
			ExpectError: true,
		},
		{
			Name: "Valid local configuration",
			Value: StorageConfig{
				Provider: "local",
				Local: &LocalStorageProviderConfig{
					Directory: "/var/lib/streamweaver",
				},
			},
			ExpectError: false,
		},
		{
			Name: "Invalid s3 configuration",
			Value: StorageConfig{
				Provider: "s3",
			},
			ExpectError: true,
		},
		{
			Name: "Invalid s3 configuration - missing bucket",
			Value: StorageConfig{
				Provider: "s3",
				S3: &AWSS3StorageProviderConfig{
					AccessKeyId:     "access_key_id",
					SecretAccessKey: "secret",
					Region:          "us-west-2",
				},
			},
			ExpectError: true,
		},
		{
			Name: "Invalid s3 configuration - missing region",
			Value: StorageConfig{
				Provider: "s3",
				S3: &AWSS3StorageProviderConfig{
					AccessKeyId:     "access_key_id",
					SecretAccessKey: "secret",
					Bucket:          "streamweaver-us-west-2",
				},
			},
			ExpectError: true,
		},
		{
			Name: "Invalid s3 configuration - missing access key",
			Value: StorageConfig{
				Provider: "s3",
				S3: &AWSS3StorageProviderConfig{
					SecretAccessKey: "secret",
					Bucket:          "streamweaver-us-west-2",
					Region:          "us-west-2",
				},
			},
			ExpectError: true,
		},
		{
			Name: "Valid s3 storage configuration",
			Value: StorageConfig{
				Provider: "s3",
				S3: &AWSS3StorageProviderConfig{
					AccessKeyId:     "access_key_id",
					SecretAccessKey: "secret",
					Bucket:          "streamweaver-us-west-2",
					Region:          "us-west-2",
				},
			},
			ExpectError: false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			err := testCase.Value.Validate()
			if (err != nil) != testCase.ExpectError {
				t.Errorf("Validate() error = %v, expectedError %v", err, testCase.ExpectError)
			}
		})
	}
}
