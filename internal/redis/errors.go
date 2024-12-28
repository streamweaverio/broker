package redis

import "fmt"

type RedisNotEnoughNodesError struct{}

type RedisStreamPublishError struct {
	Err error
}

type RedisStreamNotFoundError struct {
	Name string
}

func NotEnoughNodesError() *RedisNotEnoughNodesError {
	return &RedisNotEnoughNodesError{}
}

func StreamPublishError(err error) *RedisStreamPublishError {
	return &RedisStreamPublishError{
		Err: err,
	}
}

func StreamNotFoundError(name string) *RedisStreamNotFoundError {
	return &RedisStreamNotFoundError{
		Name: name,
	}
}

func (e *RedisNotEnoughNodesError) Error() string {
	return "Not enough nodes provided"
}

func (e *RedisStreamPublishError) Error() string {
	return fmt.Sprintf("Error publishing message: %s", e.Err)
}

func (e *RedisStreamNotFoundError) Error() string {
	return fmt.Sprintf("Stream: %s not found", e.Name)
}
