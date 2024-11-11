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
			Name: "Valid time based retention configuration",
			Value: RetentionConfig{
				MaxAge: 3600000,
			},
			ExpectError: false,
		},
		{
			Name: "Invalid time based retention configuration - native max age",
			Value: RetentionConfig{
				MaxAge: -3600000,
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
