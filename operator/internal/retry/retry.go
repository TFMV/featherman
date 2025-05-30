package retry

import (
	"context"
	"fmt"
	"time"

	"github.com/TFMV/featherman/operator/internal/logger"
	"github.com/rs/zerolog"
)

// DefaultRetryConfig provides default retry settings
var DefaultRetryConfig = RetryConfig{
	MaxRetries:      3,
	InitialInterval: 1 * time.Second,
	MaxInterval:     30 * time.Second,
	Multiplier:      2.0,
}

// RetryConfig configures the retry behavior
type RetryConfig struct {
	MaxRetries      int
	InitialInterval time.Duration
	MaxInterval     time.Duration
	Multiplier      float64
}

// WithMaxRetries sets the maximum number of retries
func (c RetryConfig) WithMaxRetries(max int) RetryConfig {
	c.MaxRetries = max
	return c
}

// WithInitialInterval sets the initial retry interval
func (c RetryConfig) WithInitialInterval(d time.Duration) RetryConfig {
	c.InitialInterval = d
	return c
}

// WithMaxInterval sets the maximum retry interval
func (c RetryConfig) WithMaxInterval(d time.Duration) RetryConfig {
	c.MaxInterval = d
	return c
}

// WithMultiplier sets the backoff multiplier
func (c RetryConfig) WithMultiplier(m float64) RetryConfig {
	c.Multiplier = m
	return c
}

// Operation represents a retryable operation
type Operation func(ctx context.Context) error

// Do executes an operation with retries using exponential backoff
func Do(ctx context.Context, op Operation, cfg RetryConfig) error {
	l := logger.FromContext(ctx)

	var lastErr error
	interval := cfg.InitialInterval

	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		// Check if context is cancelled
		if ctx.Err() != nil {
			return fmt.Errorf("operation cancelled: %w", ctx.Err())
		}

		// Execute operation
		err := op(ctx)
		if err == nil {
			if attempt > 0 {
				l.Info().
					Int("attempts", attempt+1).
					Msg("operation succeeded after retries")
			}
			return nil
		}

		lastErr = err
		if attempt == cfg.MaxRetries {
			break
		}

		// Log retry attempt
		l.Warn().
			Err(err).
			Int("attempt", attempt+1).
			Int("maxRetries", cfg.MaxRetries).
			Float64("nextIntervalSec", interval.Seconds()).
			Msg("operation failed, retrying")

		// Wait before next attempt
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("operation cancelled during retry wait: %w", ctx.Err())
		case <-timer.C:
		}

		// Calculate next interval with exponential backoff
		interval = time.Duration(float64(interval) * cfg.Multiplier)
		if interval > cfg.MaxInterval {
			interval = cfg.MaxInterval
		}
	}

	return fmt.Errorf("operation failed after %d attempts: %w", cfg.MaxRetries+1, lastErr)
}

// CircuitBreaker implements the circuit breaker pattern
type CircuitBreaker struct {
	failures     int
	maxFailures  int
	resetTimeout time.Duration
	lastFailure  time.Time
	logger       *zerolog.Logger
}

// NewCircuitBreaker creates a new circuit breaker
func NewCircuitBreaker(maxFailures int, resetTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		maxFailures:  maxFailures,
		resetTimeout: resetTimeout,
		logger:       &zerolog.Logger{},
	}
}

// Execute runs an operation through the circuit breaker
func (cb *CircuitBreaker) Execute(ctx context.Context, op Operation) error {
	if cb.isOpen() {
		return fmt.Errorf("circuit breaker is open")
	}

	err := op(ctx)
	if err != nil {
		cb.recordFailure()
		return err
	}

	cb.reset()
	return nil
}

func (cb *CircuitBreaker) isOpen() bool {
	if cb.failures >= cb.maxFailures {
		if time.Since(cb.lastFailure) > cb.resetTimeout {
			cb.reset()
			return false
		}
		return true
	}
	return false
}

func (cb *CircuitBreaker) recordFailure() {
	cb.failures++
	cb.lastFailure = time.Now()
}

func (cb *CircuitBreaker) reset() {
	cb.failures = 0
	cb.lastFailure = time.Time{}
}
