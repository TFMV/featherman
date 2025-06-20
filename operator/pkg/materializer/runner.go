package materializer

import (
	"context"
	"fmt"
	"strings"
)

// Runner executes materialization queries using DuckDB COPY
// This is a placeholder implementation and does not integrate
// with the controller yet.
type Runner struct{}

func NewRunner() *Runner { return &Runner{} }

// Copy executes the given SQL and writes results to dest
func (r *Runner) Copy(ctx context.Context, sql, dest string) error {
	if err := sanitizeSQL(sql); err != nil {
		return err
	}
	// Actual execution with DuckDB will be implemented later
	fmt.Printf("COPY (%s) TO '%s'\n", sql, dest)
	return nil
}

func sanitizeSQL(q string) error {
	upper := strings.ToUpper(q)
	banned := []string{"DROP", "ATTACH", "INSTALL"}
	for _, b := range banned {
		if strings.Contains(upper, b) {
			return fmt.Errorf("SQL contains forbidden keyword: %s", b)
		}
	}
	return nil
}
