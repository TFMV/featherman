package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

var (
	querySQL         string
	queryCatalog     string
	queryTable       string
	queryTimeout     time.Duration
	queryFromFile    string
	queryInteractive bool
)

// QueryRequest represents a SQL query request
type QueryRequest struct {
	SQL     string `json:"sql"`
	Catalog string `json:"catalog,omitempty"`
	Table   string `json:"table,omitempty"`
	Format  string `json:"format,omitempty"`
	Timeout string `json:"timeout,omitempty"`
}

// QueryResponse represents a SQL query response
type QueryResponse struct {
	Status   string     `json:"status"`
	Data     [][]string `json:"data,omitempty"`
	Columns  []string   `json:"columns,omitempty"`
	RowCount int        `json:"row_count"`
	Duration string     `json:"duration"`
	Error    string     `json:"error,omitempty"`
	QueryID  string     `json:"query_id"`
}

var queryCmd = &cobra.Command{
	Use:   "query",
	Short: "Execute SQL queries against Featherman data lake",
	Long: `Execute SQL queries against Featherman data lake catalogs and tables.

The query command allows you to run ad-hoc SQL queries against your data lake,
supporting various output formats and query sources.

Examples:
  # Run a simple query
  feathermanctl query --sql "SELECT * FROM users LIMIT 10" --catalog my-catalog
  
  # Run query from file
  feathermanctl query --file query.sql --catalog my-catalog
  
  # Query specific table with JSON output
  feathermanctl query --sql "SELECT count(*) FROM users" --catalog my-catalog --table users -o json
  
  # Interactive query mode
  feathermanctl query --interactive --catalog my-catalog`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if queryInteractive {
			return runInteractiveQuery()
		}

		// Validate required parameters
		if queryCatalog == "" {
			return fmt.Errorf("catalog is required")
		}

		var sql string
		if queryFromFile != "" {
			data, err := os.ReadFile(queryFromFile)
			if err != nil {
				return fmt.Errorf("failed to read query file %s: %w", queryFromFile, err)
			}
			sql = string(data)
		} else if querySQL != "" {
			sql = querySQL
		} else {
			return fmt.Errorf("either --sql or --file must be specified")
		}

		sql = strings.TrimSpace(sql)
		if sql == "" {
			return fmt.Errorf("SQL query cannot be empty")
		}

		Info("Executing query",
			zap.String("catalog", queryCatalog),
			zap.String("table", queryTable),
			zap.String("sql", sql))

		// Execute query
		result, err := executeQuery(sql, queryCatalog, queryTable)
		if err != nil {
			return fmt.Errorf("failed to execute query: %w", err)
		}

		// Display results
		return displayQueryResult(result)
	},
}

func init() {
	rootCmd.AddCommand(queryCmd)

	queryCmd.Flags().StringVar(&querySQL, "sql", "",
		"SQL statement to execute")
	queryCmd.Flags().StringVar(&queryCatalog, "catalog", "",
		"target catalog name (required)")
	queryCmd.Flags().StringVar(&queryTable, "table", "",
		"target table name (optional)")
	queryCmd.Flags().DurationVar(&queryTimeout, "timeout", 5*time.Minute,
		"query execution timeout")
	queryCmd.Flags().StringVar(&queryFromFile, "file", "",
		"read SQL query from file")
	queryCmd.Flags().BoolVar(&queryInteractive, "interactive", false,
		"start interactive query session")

	// Mark required flags
	queryCmd.MarkFlagRequired("catalog")

	// Make sql and file mutually exclusive with interactive
	queryCmd.MarkFlagsMutuallyExclusive("sql", "file")
	queryCmd.MarkFlagsMutuallyExclusive("interactive", "sql")
	queryCmd.MarkFlagsMutuallyExclusive("interactive", "file")
}

// executeQuery executes a SQL query against the Featherman query service
func executeQuery(sql, catalog, table string) (*QueryResponse, error) {
	endpoint := viper.GetString("query_endpoint")
	if endpoint == "" {
		return nil, fmt.Errorf("query endpoint not configured")
	}

	// Prepare request
	request := QueryRequest{
		SQL:     sql,
		Catalog: catalog,
		Table:   table,
		Format:  viper.GetString("output_format"),
		Timeout: queryTimeout.String(),
	}

	jsonData, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	// Create HTTP request
	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", endpoint+"/api/v1/query", bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", fmt.Sprintf("feathermanctl/%s", Version))

	// Execute request
	client := &http.Client{
		Timeout: queryTimeout + 10*time.Second, // Add buffer to HTTP timeout
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	// Read response
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("query failed with status %d: %s", resp.StatusCode, string(body))
	}

	// Parse response
	var result QueryResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	if result.Status == "error" {
		return nil, fmt.Errorf("query execution failed: %s", result.Error)
	}

	return &result, nil
}

// displayQueryResult displays the query result in the configured format
func displayQueryResult(result *QueryResponse) error {
	// Display metadata
	outputter.PrintInfo(fmt.Sprintf("Query ID: %s", result.QueryID))
	outputter.PrintInfo(fmt.Sprintf("Duration: %s", result.Duration))
	outputter.PrintInfo(fmt.Sprintf("Rows: %d", result.RowCount))

	if result.RowCount == 0 {
		outputter.PrintWarning("No rows returned")
		return nil
	}

	// Display data
	return outputter.PrintTable(result.Columns, result.Data)
}

// runInteractiveQuery starts an interactive query session
func runInteractiveQuery() error {
	outputter.PrintInfo("Starting interactive query session")
	outputter.PrintInfo("Type 'exit' or 'quit' to end the session")
	outputter.PrintInfo(fmt.Sprintf("Connected to catalog: %s", queryCatalog))

	fmt.Print("\nfeatherman> ")

	// This is a simplified implementation - in a real scenario,
	// you would use a proper readline library like github.com/chzyer/readline
	// for better interactive experience with history, completion, etc.

	var input string
	for {
		_, err := fmt.Scanln(&input)
		if err != nil {
			if err == io.EOF {
				break
			}
			Error("Failed to read input", zap.Error(err))
			continue
		}

		input = strings.TrimSpace(input)
		if input == "" {
			fmt.Print("featherman> ")
			continue
		}

		if input == "exit" || input == "quit" {
			break
		}

		// Execute query
		result, err := executeQuery(input, queryCatalog, queryTable)
		if err != nil {
			outputter.PrintError(fmt.Sprintf("Query failed: %v", err))
		} else {
			displayQueryResult(result)
		}

		fmt.Print("\nfeatherman> ")
	}

	outputter.PrintInfo("Interactive session ended")
	return nil
}
