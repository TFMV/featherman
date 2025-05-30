package logger

import (
	"context"
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func init() {
	// Configure global logger
	zerolog.TimeFieldFormat = time.RFC3339Nano
	log.Logger = log.Output(zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: time.RFC3339Nano,
	})

	// Set log level based on environment
	if os.Getenv("DEBUG") == "true" {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	} else {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}
}

// FromContext returns a logger from the given context
func FromContext(ctx context.Context) *zerolog.Logger {
	if ctx == nil {
		return &log.Logger
	}
	if l, ok := ctx.Value(loggerKey{}).(*zerolog.Logger); ok {
		return l
	}
	return &log.Logger
}

// WithContext returns a new context with the given logger
func WithContext(ctx context.Context, l *zerolog.Logger) context.Context {
	return context.WithValue(ctx, loggerKey{}, l)
}

// WithValues returns a new logger with the given values
func WithValues(l *zerolog.Logger, keysAndValues ...interface{}) *zerolog.Logger {
	if len(keysAndValues)%2 != 0 {
		keysAndValues = append(keysAndValues, "MISSING_VALUE")
	}
	logger := l.With()
	for i := 0; i < len(keysAndValues); i += 2 {
		key, ok := keysAndValues[i].(string)
		if !ok {
			key = "INVALID_KEY"
		}
		logger = logger.Interface(key, keysAndValues[i+1])
	}
	newLogger := logger.Logger()
	return &newLogger
}

// WithError returns a new logger with the given error
func WithError(l *zerolog.Logger, err error) *zerolog.Logger {
	if err == nil {
		return l
	}
	logger := l.With().Err(err).Logger()
	return &logger
}

type loggerKey struct{}
