package config

import "testing"

type RetentionConfigTestCase struct {
	Name        string          `json:"name"`
	Value       RetentionConfig `json:"config"`
	ExpectError bool            `json:"expectedError"`
}

func TestRetentionConfig_Validate(t *testing.T) {
	testCases := []RetentionConfigTestCase{
		{
			Name: "Invalid retention policy",
			Value: RetentionConfig{
				Policy: "custom",
			},
			ExpectError: true,
		},
		{
			Name: "Valid time based retention configuration",
			Value: RetentionConfig{
				Policy: "time",
				MaxAge: "1d",
			},
			ExpectError: false,
		},
		{
			Name: "Invalid time based retention configuration - invalid max_age format",
			Value: RetentionConfig{
				Policy: "time",
				MaxAge: "-1d",
			},
			ExpectError: true,
		},
		{
			Name: "Invalid time based retention configuration - missing max age",
			Value: RetentionConfig{
				Policy: "time",
			},
			ExpectError: true,
		},
		{
			Name: "Valid size based retention configuration",
			Value: RetentionConfig{
				Policy:  "size",
				MaxSize: 1024,
			},
			ExpectError: false,
		},
		{
			Name: "Invalid size based retention configuration - invalid max size",
			Value: RetentionConfig{
				Policy:  "size",
				MaxSize: 0,
			},
			ExpectError: true,
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
