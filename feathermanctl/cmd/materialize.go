package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

var (
	materializeSource      string
	materializeDestination string
	materializeFormat      string
	materializeCompression string
	materializeTimeout     time.Duration
	materializeOverwrite   bool
	materializePartitions  []string
	materializeBatchSize   int
)

// MaterializeRequest represents a materialization request
type MaterializeRequest struct {
	Source      string            `json:"source"`
	Destination string            `json:"destination"`
	Format      string            `json:"format"`
	Compression string            `json:"compression"`
	Overwrite   bool              `json:"overwrite"`
	Partitions  []string          `json:"partitions,omitempty"`
	BatchSize   int               `json:"batch_size"`
	Options     map[string]string `json:"options,omitempty"`
}

// MaterializeResponse represents a materialization response
type MaterializeResponse struct {
	JobID          string `json:"job_id"`
	Status         string `json:"status"`
	Message        string `json:"message"`
	RecordsWritten int64  `json:"records_written"`
	BytesWritten   int64  `json:"bytes_written"`
	Duration       string `json:"duration"`
	Error          string `json:"error,omitempty"`
}

// MaterializeManifest represents a materialization manifest file
type MaterializeManifest struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
	Metadata   struct {
		Name string `yaml:"name"`
	} `yaml:"metadata"`
	Spec struct {
		Source      string            `yaml:"source"`
		Destination string            `yaml:"destination"`
		Format      string            `yaml:"format"`
		Compression string            `yaml:"compression,omitempty"`
		Overwrite   bool              `yaml:"overwrite,omitempty"`
		Partitions  []string          `yaml:"partitions,omitempty"`
		BatchSize   int               `yaml:"batchSize,omitempty"`
		Options     map[string]string `yaml:"options,omitempty"`
	} `yaml:"spec"`
}

var materializeCmd = &cobra.Command{
	Use:   "materialize [manifest-file]",
	Short: "Materialize views and tables to storage",
	Long: `Materialize Featherman views and tables to various storage formats and destinations.

The materialize command takes a logical view or table and converts it to a physical
storage format, supporting various destinations and optimizations.

Examples:
  # Materialize using a manifest file
  feathermanctl materialize materialization.yaml
  
  # Materialize with command-line options
  feathermanctl materialize --source "SELECT * FROM my_catalog.my_table" --destination "s3://bucket/path/"
  
  # Materialize with specific format and compression
  feathermanctl materialize --source "my_catalog.my_table" --destination "./output/" --format parquet --compression snappy
  
  # Materialize with partitioning
  feathermanctl materialize --source "my_catalog.sales" --destination "s3://bucket/sales/" --partitions "year,month"`,
	Args: cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		var request MaterializeRequest

		if len(args) == 1 {
			// Load from manifest file
			manifestPath := args[0]
			manifest, err := loadMaterializeManifest(manifestPath)
			if err != nil {
				return fmt.Errorf("failed to load manifest: %w", err)
			}
			request = convertManifestToRequest(manifest)
		} else {
			// Use command-line arguments
			if materializeSource == "" || materializeDestination == "" {
				return fmt.Errorf("source and destination are required when not using a manifest file")
			}

			request = MaterializeRequest{
				Source:      materializeSource,
				Destination: materializeDestination,
				Format:      materializeFormat,
				Compression: materializeCompression,
				Overwrite:   materializeOverwrite,
				Partitions:  materializePartitions,
				BatchSize:   materializeBatchSize,
			}
		}

		// Validate request
		if err := validateMaterializeRequest(&request); err != nil {
			return fmt.Errorf("invalid materialization request: %w", err)
		}

		Info("Starting materialization",
			zap.String("source", request.Source),
			zap.String("destination", request.Destination),
			zap.String("format", request.Format))

		// Execute materialization
		response, err := executeMaterialization(&request)
		if err != nil {
			return fmt.Errorf("materialization failed: %w", err)
		}

		// Display results
		return displayMaterializationResult(response)
	},
}

func init() {
	rootCmd.AddCommand(materializeCmd)

	materializeCmd.Flags().StringVar(&materializeSource, "source", "",
		"source table, view, or SQL query")
	materializeCmd.Flags().StringVar(&materializeDestination, "destination", "",
		"destination path or URI")
	materializeCmd.Flags().StringVar(&materializeFormat, "format", "parquet",
		"output format (parquet, csv, json, orc)")
	materializeCmd.Flags().StringVar(&materializeCompression, "compression", "snappy",
		"compression algorithm (snappy, gzip, lz4, zstd)")
	materializeCmd.Flags().BoolVar(&materializeOverwrite, "overwrite", false,
		"overwrite existing data")
	materializeCmd.Flags().StringSliceVar(&materializePartitions, "partitions", []string{},
		"partition columns")
	materializeCmd.Flags().IntVar(&materializeBatchSize, "batch-size", 10000,
		"batch size for processing")
	materializeCmd.Flags().DurationVar(&materializeTimeout, "timeout", 30*time.Minute,
		"materialization timeout")
}

func loadMaterializeManifest(path string) (*MaterializeManifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read manifest file: %w", err)
	}

	var manifest MaterializeManifest
	if err := yaml.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("failed to parse YAML manifest: %w", err)
	}

	// Validate manifest structure
	if manifest.APIVersion == "" {
		return nil, fmt.Errorf("missing apiVersion in manifest")
	}
	if manifest.Kind != "Materialization" {
		return nil, fmt.Errorf("invalid kind: expected 'Materialization', got '%s'", manifest.Kind)
	}
	if manifest.Spec.Source == "" {
		return nil, fmt.Errorf("missing source in manifest spec")
	}
	if manifest.Spec.Destination == "" {
		return nil, fmt.Errorf("missing destination in manifest spec")
	}

	return &manifest, nil
}

func convertManifestToRequest(manifest *MaterializeManifest) MaterializeRequest {
	request := MaterializeRequest{
		Source:      manifest.Spec.Source,
		Destination: manifest.Spec.Destination,
		Format:      manifest.Spec.Format,
		Compression: manifest.Spec.Compression,
		Overwrite:   manifest.Spec.Overwrite,
		Partitions:  manifest.Spec.Partitions,
		BatchSize:   manifest.Spec.BatchSize,
		Options:     manifest.Spec.Options,
	}

	// Set defaults
	if request.Format == "" {
		request.Format = "parquet"
	}
	if request.Compression == "" {
		request.Compression = "snappy"
	}
	if request.BatchSize == 0 {
		request.BatchSize = 10000
	}

	return request
}

func validateMaterializeRequest(request *MaterializeRequest) error {
	if request.Source == "" {
		return fmt.Errorf("source cannot be empty")
	}
	if request.Destination == "" {
		return fmt.Errorf("destination cannot be empty")
	}

	// Validate format
	validFormats := map[string]bool{
		"parquet": true,
		"csv":     true,
		"json":    true,
		"orc":     true,
		"delta":   true,
	}
	if !validFormats[request.Format] {
		return fmt.Errorf("unsupported format: %s", request.Format)
	}

	// Validate compression
	validCompressions := map[string]bool{
		"snappy": true,
		"gzip":   true,
		"lz4":    true,
		"zstd":   true,
		"none":   true,
	}
	if request.Compression != "" && !validCompressions[request.Compression] {
		return fmt.Errorf("unsupported compression: %s", request.Compression)
	}

	// Validate batch size
	if request.BatchSize <= 0 {
		return fmt.Errorf("batch size must be positive")
	}

	return nil
}

func executeMaterialization(request *MaterializeRequest) (*MaterializeResponse, error) {
	endpoint := viper.GetString("query_endpoint")
	if endpoint == "" {
		return nil, fmt.Errorf("query endpoint not configured")
	}

	// Marshal request
	jsonData, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	// Create HTTP request
	ctx, cancel := context.WithTimeout(context.Background(), materializeTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", endpoint+"/api/v1/materialize", bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", fmt.Sprintf("feathermanctl/%s", Version))

	// Execute request
	client := &http.Client{
		Timeout: materializeTimeout + 10*time.Second,
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

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return nil, fmt.Errorf("materialization failed with status %d: %s", resp.StatusCode, string(body))
	}

	// Parse response
	var response MaterializeResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	if response.Status == "error" {
		return nil, fmt.Errorf("materialization failed: %s", response.Error)
	}

	// If async job, poll for completion
	if response.Status == "accepted" && response.JobID != "" {
		return pollMaterializationJob(response.JobID)
	}

	return &response, nil
}

func pollMaterializationJob(jobID string) (*MaterializeResponse, error) {
	endpoint := viper.GetString("query_endpoint")
	pollInterval := 5 * time.Second
	maxPolls := int(materializeTimeout / pollInterval)

	outputter.PrintInfo(fmt.Sprintf("Materialization job started: %s", jobID))
	outputter.PrintInfo("Polling for completion...")

	for i := 0; i < maxPolls; i++ {
		time.Sleep(pollInterval)

		// Check job status
		resp, err := http.Get(fmt.Sprintf("%s/api/v1/jobs/%s", endpoint, jobID))
		if err != nil {
			Warn("Failed to poll job status", zap.Error(err))
			continue
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			Warn("Failed to read job status response", zap.Error(err))
			continue
		}

		var response MaterializeResponse
		if err := json.Unmarshal(body, &response); err != nil {
			Warn("Failed to parse job status response", zap.Error(err))
			continue
		}

		switch response.Status {
		case "completed":
			outputter.PrintSuccess("Materialization completed")
			return &response, nil
		case "failed":
			return nil, fmt.Errorf("materialization job failed: %s", response.Error)
		case "running":
			Info("Materialization in progress", zap.String("job_id", jobID))
		}
	}

	return nil, fmt.Errorf("materialization job timed out after %v", materializeTimeout)
}

func displayMaterializationResult(response *MaterializeResponse) error {
	outputter.PrintSuccess("Materialization completed successfully")

	// Display summary
	headers := []string{"Metric", "Value"}
	rows := [][]string{
		{"Job ID", response.JobID},
		{"Status", response.Status},
		{"Records Written", fmt.Sprintf("%d", response.RecordsWritten)},
		{"Bytes Written", formatBytes(response.BytesWritten)},
		{"Duration", response.Duration},
	}

	if response.Message != "" {
		rows = append(rows, []string{"Message", response.Message})
	}

	return outputter.PrintTable(headers, rows)
}

func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
