package retention

import "fmt"

type RetentionPolicyOptionNotSetError struct {
	Option string
}

func PolicyOptionNotSetError(option string) *RetentionPolicyOptionNotSetError {
	return &RetentionPolicyOptionNotSetError{
		Option: option,
	}
}

func (e *RetentionPolicyOptionNotSetError) Error() string {
	return fmt.Sprintf("%s option not set", e.Option)
}
