package logging

import (
	"fmt"
	"os"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type LoggerContract interface {
	Debug(msg string, fields ...zapcore.Field)
	Info(msg string, fields ...zapcore.Field)
	Warn(msg string, fields ...zapcore.Field)
	Error(msg string, fields ...zapcore.Field)
	Fatal(msg string, fields ...zapcore.Field)
}

type Logger struct {
	_Logger *zap.Logger
}

type LoggerOptions struct {
	// Log level: debug, info, warn, error, fatal
	LogLevel string
	// Where to output logs; console, file
	LogOutput string
	// Log output format: text, json
	LogFormat string
	// Log file prefix (only used when LogOutput is file)
	LogFilePrefix string
	// Log directory (only used when LogOutput is file)
	LogDirectory string
	// Max file size in MB (only used when LogOutput is file)
	MaxFileSize int
}

func NewLogger(opts *LoggerOptions) (*Logger, error) {
	zapLogger, err := NewZapLogger(opts)
	if err != nil {
		return nil, err
	}

	return &Logger{_Logger: zapLogger}, nil
}

// Create a new zap logger
func NewZapLogger(opts *LoggerOptions) (*zap.Logger, error) {
	encoderConfig := zap.NewProductionEncoderConfig()

	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	encoderConfig.TimeKey = "timestamp"
	encoderConfig.CallerKey = "caller"

	var encoder zapcore.Encoder
	switch strings.ToLower(opts.LogFormat) {
	case "text":
		encoder = zapcore.NewConsoleEncoder(encoderConfig)
	case "json":
		encoder = zapcore.NewJSONEncoder(encoderConfig)
	default:
		return nil, fmt.Errorf("invalid log format: must be 'json' or 'text'")
	}

	// Configure output
	var writeSyncer zapcore.WriteSyncer
	switch strings.ToLower(opts.LogOutput) {
	case "console":
		writeSyncer = zapcore.AddSync(os.Stdout)
	case "file":
		fileLogger, err := NewFileLogger(&FileLoggerOptions{
			LogDirectory:  opts.LogDirectory,
			LogFilePrefix: opts.LogFilePrefix,
			MaxSize:       opts.MaxFileSize,
			MaxAge:        7,
		})
		if err != nil {
			return nil, err
		}
		writeSyncer = zapcore.AddSync(fileLogger)
	default:
		return nil, fmt.Errorf("invalid log output: must be 'console' or 'file'")
	}

	core := zapcore.NewCore(
		encoder,
		writeSyncer,
		ParseLogLevel(opts.LogLevel),
	)

	return zap.New(core), nil
}

func ParseLogLevel(level string) zap.AtomicLevel {
	// ensure the level is uppercase
	level = strings.ToUpper(level)

	switch level {
	case "DEBUG":
		return zap.NewAtomicLevelAt(zapcore.DebugLevel)
	case "INFO":
		return zap.NewAtomicLevelAt(zapcore.InfoLevel)
	case "WARN":
		return zap.NewAtomicLevelAt(zapcore.WarnLevel)
	case "ERROR":
		return zap.NewAtomicLevelAt(zapcore.ErrorLevel)
	case "FATAL":
		return zap.NewAtomicLevelAt(zapcore.FatalLevel)
	default:
		return zap.NewAtomicLevelAt(zapcore.InfoLevel)
	}
}

func (l *Logger) Debug(msg string, fields ...zapcore.Field) {
	l._Logger.Debug(msg, fields...)
}

func (l *Logger) Info(msg string, fields ...zapcore.Field) {
	l._Logger.Info(msg, fields...)
}

func (l *Logger) Warn(msg string, fields ...zapcore.Field) {
	l._Logger.Warn(msg, fields...)
}

func (l *Logger) Error(msg string, fields ...zapcore.Field) {
	l._Logger.Error(msg, fields...)
}

func (l *Logger) Fatal(msg string, fields ...zapcore.Field) {
	l._Logger.Fatal(msg, fields...)
}
