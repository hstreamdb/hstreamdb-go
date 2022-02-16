package util

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"sync/atomic"
)

var globalLogger atomic.Value

type LogLevel = zapcore.Level

const (
	PANIC   LogLevel = zapcore.PanicLevel
	FATAL   LogLevel = zapcore.FatalLevel
	ERROR   LogLevel = zapcore.ErrorLevel
	WARNING LogLevel = zapcore.WarnLevel
	INFO    LogLevel = zapcore.InfoLevel
	DEBUG   LogLevel = zapcore.DebugLevel
)

func init() {
	logger, _ := InitLogger(DEBUG)
	ReplaceGlobals(logger)
}

// InitLogger initializes a zap logger.
func InitLogger(level LogLevel, opts ...zap.Option) (*zap.Logger, error) {
	stdOut, _, err := zap.Open([]string{"stdout"}...)
	if err != nil {
		return nil, err
	}
	cfg := zap.NewProductionConfig()
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(cfg.EncoderConfig),
		zapcore.AddSync(stdOut),
		zap.NewAtomicLevelAt(level),
	)
	return zap.New(core), nil
}

// Logger returns the global Logger. It's safe for concurrent use.
func Logger() *zap.Logger {
	return globalLogger.Load().(*zap.Logger)
}

// ReplaceGlobals replaces the global Logger. It's safe for concurrent use.
func ReplaceGlobals(logger *zap.Logger) {
	globalLogger.Store(logger)
}

// Sync flushes any buffered log entries.
func Sync() error {
	return Logger().Sync()
}
