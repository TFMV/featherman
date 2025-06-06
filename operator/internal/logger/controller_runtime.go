package logger

import (
	"github.com/go-logr/logr"
	"github.com/rs/zerolog"
)

// ControllerRuntimeAdapter adapts our zerolog-based logger to controller-runtime's logging interface
type ControllerRuntimeAdapter struct {
	name    string
	level   int
	context map[string]interface{}
}

// Enabled implements logr.LogSink
func (a *ControllerRuntimeAdapter) Enabled(level int) bool {
	// Map controller-runtime verbosity levels to our log levels
	// Controller-runtime uses higher numbers for more verbose logs
	// We'll consider anything above level 1 as debug
	if level > 1 {
		return zerolog.GlobalLevel() <= zerolog.DebugLevel
	}
	return true
}

// Info implements logr.LogSink
func (a *ControllerRuntimeAdapter) Info(level int, msg string, keysAndValues ...interface{}) {
	event := Info()
	if level > 1 {
		event = Debug()
	}

	// Add context fields
	for k, v := range a.context {
		event.Interface(k, v)
	}

	// Add name if set
	if a.name != "" {
		event.Str("logger", a.name)
	}

	// Add key-value pairs
	for i := 0; i < len(keysAndValues); i += 2 {
		key, ok := keysAndValues[i].(string)
		if !ok {
			key = "unknown_key"
		}
		var value interface{}
		if i+1 < len(keysAndValues) {
			value = keysAndValues[i+1]
		} else {
			value = "missing_value"
		}
		event.Interface(key, value)
	}

	event.Msg(msg)
}

// Error implements logr.LogSink
func (a *ControllerRuntimeAdapter) Error(err error, msg string, keysAndValues ...interface{}) {
	event := Error().Err(err)

	// Add context fields
	for k, v := range a.context {
		event.Interface(k, v)
	}

	// Add name if set
	if a.name != "" {
		event.Str("logger", a.name)
	}

	// Add key-value pairs
	for i := 0; i < len(keysAndValues); i += 2 {
		key, ok := keysAndValues[i].(string)
		if !ok {
			key = "unknown_key"
		}
		var value interface{}
		if i+1 < len(keysAndValues) {
			value = keysAndValues[i+1]
		} else {
			value = "missing_value"
		}
		event.Interface(key, value)
	}

	event.Msg(msg)
}

// WithValues implements logr.LogSink
func (a *ControllerRuntimeAdapter) WithValues(keysAndValues ...interface{}) logr.LogSink {
	newContext := make(map[string]interface{}, len(a.context)+len(keysAndValues)/2)
	for k, v := range a.context {
		newContext[k] = v
	}

	for i := 0; i < len(keysAndValues); i += 2 {
		key, ok := keysAndValues[i].(string)
		if !ok {
			key = "unknown_key"
		}
		var value interface{}
		if i+1 < len(keysAndValues) {
			value = keysAndValues[i+1]
		} else {
			value = "missing_value"
		}
		newContext[key] = value
	}

	return &ControllerRuntimeAdapter{
		name:    a.name,
		level:   a.level,
		context: newContext,
	}
}

// WithName implements logr.LogSink
func (a *ControllerRuntimeAdapter) WithName(name string) logr.LogSink {
	var newName string
	if a.name == "" {
		newName = name
	} else {
		newName = a.name + "." + name
	}

	return &ControllerRuntimeAdapter{
		name:    newName,
		level:   a.level,
		context: a.context,
	}
}

// Init implements logr.LogSink
func (a *ControllerRuntimeAdapter) Init(info logr.RuntimeInfo) {
	// No initialization needed
}

// NewControllerRuntimeLogger creates a new logr.Logger that uses our zerolog-based logger
func NewControllerRuntimeLogger() logr.Logger {
	return logr.New(&ControllerRuntimeAdapter{
		context: make(map[string]interface{}),
	})
}
