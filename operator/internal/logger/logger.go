package logger

import (
	"context"
	"io"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// -----------------------------
// Configuration and Initialization
// -----------------------------

// Config holds configuration parameters for the global logger.
// All fields are optional; environment variables will be used as defaults if unset.
type Config struct {
	// ServiceName is an application/service identifier to be included in every log line.
	ServiceName string

	// Environment (e.g., "production", "staging", "development").
	Environment string

	// LogLevel overrides the default log level (Info). Accepts "debug", "info",
	// "warn", "error", "fatal", "panic". Case-insensitive.
	LogLevel string

	// Format can be "json" or "console". Defaults to "console" if unset.
	Format string

	// TimeFormat sets the timestamp format. Defaults to time.RFC3339Nano.
	TimeFormat string

	// Output defines the io.Writer for logs. Defaults to os.Stdout.
	Output io.Writer
}

var (
	defaultTimeFormat   = time.RFC3339Nano
	defaultConfig       Config
	initializeOnce      sync.Once
	globalLogger        zerolog.Logger
	globalContextLogger *zerolog.Logger
)

// init is called on package import. It triggers default initialization.
// For custom settings, users should call Configure before logging any messages.
func init() {
	Configure(defaultConfig)
}

// Configure allows overriding default logger settings. It may be called once
// at application startup. Subsequent calls have no effect.
func Configure(cfg Config) {
	initializeOnce.Do(func() {
		// Merge defaults from environment if fields are unset.
		if cfg.ServiceName == "" {
			cfg.ServiceName = os.Getenv("SERVICE_NAME")
		}
		if cfg.Environment == "" {
			cfg.Environment = os.Getenv("ENVIRONMENT")
		}
		if cfg.LogLevel == "" {
			cfg.LogLevel = os.Getenv("LOG_LEVEL")
		}
		if cfg.Format == "" {
			cfg.Format = os.Getenv("LOG_FORMAT")
		}
		if cfg.TimeFormat == "" {
			cfg.TimeFormat = os.Getenv("LOG_TIME_FORMAT")
			if cfg.TimeFormat == "" {
				cfg.TimeFormat = defaultTimeFormat
			}
		}
		if cfg.Output == nil {
			cfg.Output = os.Stdout
		}

		// Set zerolog time format
		zerolog.TimeFieldFormat = cfg.TimeFormat

		// Choose output format
		var writer io.Writer
		switch cfg.Format {
		case "json", "JSON", "Json":
			// JSON format: no additional wrapping
			writer = cfg.Output
		default:
			// Console (human-friendly) format
			writer = zerolog.ConsoleWriter{
				Out:        cfg.Output,
				TimeFormat: cfg.TimeFormat,
			}
		}

		// Build base logger
		base := zerolog.New(writer).
			With().
			Timestamp().
			Str("service", cfg.ServiceName).
			Str("env", cfg.Environment).
			Logger().
			Hook(callerHook{}) // include caller file and line number

		// Set global log level
		level := parseLogLevel(cfg.LogLevel)
		zerolog.SetGlobalLevel(level)
		base = base.Level(level)

		globalLogger = base
		globalContextLogger = &globalLogger

		// Replace the default zerolog logger so that direct calls to log.* work.
		log.Logger = globalLogger
	})
}

// parseLogLevel maps a string to a zerolog.Level. Defaults to InfoLevel.
func parseLogLevel(levelStr string) zerolog.Level {
	switch zerolog.GlobalLevel() { // if already set by someone else, keep it
	case zerolog.DebugLevel, zerolog.InfoLevel, zerolog.WarnLevel, zerolog.ErrorLevel, zerolog.FatalLevel, zerolog.PanicLevel:
		// do nothing; level already set
	default:
		// parse explicit levels
		switch levelStr {
		case "debug", "Debug", "DEBUG":
			return zerolog.DebugLevel
		case "info", "Info", "INFO":
			return zerolog.InfoLevel
		case "warn", "Warn", "WARN", "warning", "Warning", "WARNING":
			return zerolog.WarnLevel
		case "error", "Error", "ERROR":
			return zerolog.ErrorLevel
		case "fatal", "Fatal", "FATAL":
			return zerolog.FatalLevel
		case "panic", "Panic", "PANIC":
			return zerolog.PanicLevel
		default:
			return zerolog.InfoLevel
		}
	}
	return zerolog.GlobalLevel()
}

// callerHook adds file and line number to each event.
type callerHook struct{}

func (h callerHook) Run(e *zerolog.Event, level zerolog.Level, msg string) {
	// Skip 3 stack frames to reach the caller of the logger method.
	_, file, line, ok := runtime.Caller(3)
	if ok {
		e.Str("caller", file+":"+strconv.Itoa(line))
	}
}

// -----------------------------
// Contextual Logging Utilities
// -----------------------------

// loggerKey is a private type to avoid context key collisions.
type loggerKey struct{}

// FromContext returns a logger from the given context. If no logger is found,
// it returns the global logger.
func FromContext(ctx context.Context) *zerolog.Logger {
	if ctx == nil {
		return globalContextLogger
	}
	if l, ok := ctx.Value(loggerKey{}).(*zerolog.Logger); ok {
		return l
	}
	return globalContextLogger
}

// WithContext returns a new context that carries the provided logger.
func WithContext(ctx context.Context, l *zerolog.Logger) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, loggerKey{}, l)
}

// WithValues returns a new logger with the provided key/value pairs
// attached as structured fields. Keys must be strings; non-string keys
// will be replaced with "invalid_key". If an odd number of arguments is
// passed, the last value is labeled "MISSING_VALUE".
func WithValues(l *zerolog.Logger, keysAndValues ...interface{}) *zerolog.Logger {
	if len(keysAndValues)%2 != 0 {
		keysAndValues = append(keysAndValues, "MISSING_VALUE")
	}
	evt := l.With()
	for i := 0; i < len(keysAndValues); i += 2 {
		rawKey := keysAndValues[i]
		rawVal := keysAndValues[i+1]
		key, ok := rawKey.(string)
		if !ok || key == "" {
			key = "invalid_key"
		}
		evt = evt.Interface(key, rawVal)
	}
	newLogger := evt.Logger()
	return &newLogger
}

// WithError returns a new logger with the provided error attached.
// If err is nil, returns the original logger.
func WithError(l *zerolog.Logger, err error) *zerolog.Logger {
	if err == nil {
		return l
	}
	newLogger := l.With().Err(err).Logger()
	return &newLogger
}

// -----------------------------
// Convenient Global Logging Functions
// -----------------------------

// Debug logs a message at DebugLevel using the global logger.
func Debug() *zerolog.Event { return globalLogger.Debug() }

// Info logs a message at InfoLevel using the global logger.
func Info() *zerolog.Event { return globalLogger.Info() }

// Warn logs a message at WarnLevel using the global logger.
func Warn() *zerolog.Event { return globalLogger.Warn() }

// Error logs a message at ErrorLevel using the global logger.
func Error() *zerolog.Event { return globalLogger.Error() }

// Fatal logs a message at FatalLevel using the global logger, then exits.
func Fatal() *zerolog.Event { return globalLogger.Fatal() }

// Panic logs a message at PanicLevel using the global logger, then panics.
func Panic() *zerolog.Event { return globalLogger.Panic() }

// Helper to format integer line numbers as strings
// (since zerolog requires string conversion)
func Itoa(i int) string {
	return strconv.Itoa(i)
}
