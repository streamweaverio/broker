package redis

type RedisNotEnoughNodesError struct{}

func NotEnoughNodesError() *RedisNotEnoughNodesError {
	return &RedisNotEnoughNodesError{}
}

func (e *RedisNotEnoughNodesError) Error() string {
	return "Not enough nodes provided"
}
