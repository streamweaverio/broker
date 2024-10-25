package config

import "testing"

type RedisConfigTestCase struct {
	Name        string      `json:"name"`
	Value       RedisConfig `json:"config"`
	ExpectError bool        `json:"expectedError"`
}

var testCases = []RedisConfigTestCase{
	{
		Name: "Valid configuration",
		Value: RedisConfig{
			Hosts: []RedisHostConfig{
				{
					Host: "localhost",
					Port: 6379,
				},
			},
			DB: 0,
		},
	},
	{
		Name: "Missing host",
		Value: RedisConfig{
			Hosts: []RedisHostConfig{
				{
					Port: 6379,
				},
			},
			DB: 0,
		},
		ExpectError: true,
	},
	{
		Name: "No hosts",
		Value: RedisConfig{
			Hosts:    []RedisHostConfig{},
			DB:       0,
			Password: "password",
		},
		ExpectError: true,
	},
}

func TestRedisConfig_Validate(t *testing.T) {
	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			err := testCase.Value.Validate()
			if (err != nil) != testCase.ExpectError {
				t.Errorf("Validate() error = %v, expectedError %v", err, testCase.ExpectError)
			}
		})
	}
}
