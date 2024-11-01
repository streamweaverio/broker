package testutils

import "go.uber.org/zap/zapcore"

type MockLogger struct{}

func NewMockLogger() *MockLogger {
	return &MockLogger{}
}

func (l *MockLogger) Info(msg string, fields ...zapcore.Field)  {}
func (l *MockLogger) Debug(msg string, fields ...zapcore.Field) {}
func (l *MockLogger) Warn(msg string, fields ...zapcore.Field)  {}
func (l *MockLogger) Error(msg string, fields ...zapcore.Field) {}
func (l *MockLogger) Fatal(msg string, fields ...zapcore.Field) {}
