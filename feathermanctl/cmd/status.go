package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	statusCatalog       string
	statusTable         string
	statusPool          string
	statusAll           bool
	statusWatch         bool
	statusWatchInterval time.Duration
)

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show status of Featherman lake resources",
	Long: `Display the status of Featherman data lake resources including catalogs, tables, and pools.

The status command provides detailed information about the current state of your
data lake resources, including health, readiness, and operational status.

Examples:
  # Show status of all resources
  feathermanctl status --all
  
  # Show status of specific catalog
  feathermanctl status --catalog my-catalog
  
  # Show status of specific table
  feathermanctl status --catalog my-catalog --table my-table
  
  # Show status of specific pool
  feathermanctl status --pool my-pool
  
  # Watch status changes
  feathermanctl status --all --watch`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if !statusAll && statusCatalog == "" && statusTable == "" && statusPool == "" {
			return fmt.Errorf("specify --all or provide specific resource filters")
		}

		if statusWatch {
			return watchStatus()
		}

		return showStatus()
	},
}

func init() {
	rootCmd.AddCommand(statusCmd)

	statusCmd.Flags().StringVar(&statusCatalog, "catalog", "",
		"show status for specific catalog")
	statusCmd.Flags().StringVar(&statusTable, "table", "",
		"show status for specific table (requires --catalog)")
	statusCmd.Flags().StringVar(&statusPool, "pool", "",
		"show status for specific pool")
	statusCmd.Flags().BoolVar(&statusAll, "all", false,
		"show status for all resources")
	statusCmd.Flags().BoolVar(&statusWatch, "watch", false,
		"watch for status changes")
	statusCmd.Flags().DurationVar(&statusWatchInterval, "watch-interval", 5*time.Second,
		"interval for watch updates")

	// Table requires catalog
	statusCmd.MarkFlagsRequiredTogether("table", "catalog")
}

// ResourceStatus represents the status of a Featherman resource
type ResourceStatus struct {
	Kind      string
	Name      string
	Namespace string
	Status    string
	Ready     string
	Age       string
	Message   string
}

func showStatus() error {
	Info("Fetching resource status",
		zap.String("catalog", statusCatalog),
		zap.String("table", statusTable),
		zap.String("pool", statusPool),
		zap.Bool("all", statusAll))

	// Get dynamic client
	dynamicClient, err := getKubernetesDynamicClient()
	if err != nil {
		return fmt.Errorf("failed to create Kubernetes client: %w", err)
	}

	var resources []ResourceStatus
	namespace := viper.GetString("namespace")

	// Fetch catalog status
	if statusAll || statusCatalog != "" {
		catalogResources, err := getCatalogStatus(dynamicClient, namespace, statusCatalog)
		if err != nil {
			Error("Failed to get catalog status", zap.Error(err))
		} else {
			resources = append(resources, catalogResources...)
		}
	}

	// Fetch table status
	if statusAll || statusTable != "" {
		tableResources, err := getTableStatus(dynamicClient, namespace, statusCatalog, statusTable)
		if err != nil {
			Error("Failed to get table status", zap.Error(err))
		} else {
			resources = append(resources, tableResources...)
		}
	}

	// Fetch pool status
	if statusAll || statusPool != "" {
		poolResources, err := getPoolStatus(dynamicClient, namespace, statusPool)
		if err != nil {
			Error("Failed to get pool status", zap.Error(err))
		} else {
			resources = append(resources, poolResources...)
		}
	}

	if len(resources) == 0 {
		outputter.PrintWarning("No resources found")
		return nil
	}

	// Display results
	headers := []string{"Kind", "Name", "Namespace", "Status", "Ready", "Age", "Message"}
	var rows [][]string

	for _, resource := range resources {
		rows = append(rows, []string{
			resource.Kind,
			resource.Name,
			resource.Namespace,
			resource.Status,
			resource.Ready,
			resource.Age,
			resource.Message,
		})
	}

	return outputter.PrintTable(headers, rows)
}

func watchStatus() error {
	outputter.PrintInfo(fmt.Sprintf("Watching status changes (interval: %v)...", statusWatchInterval))
	outputter.PrintInfo("Press Ctrl+C to stop watching")

	ticker := time.NewTicker(statusWatchInterval)
	defer ticker.Stop()

	// Show initial status
	if err := showStatus(); err != nil {
		return err
	}

	for {
		select {
		case <-ticker.C:
			fmt.Println("\n" + strings.Repeat("=", 80))
			fmt.Printf("Status Update - %s\n", time.Now().Format("15:04:05"))
			fmt.Println(strings.Repeat("=", 80))

			if err := showStatus(); err != nil {
				Error("Failed to refresh status", zap.Error(err))
			}
		}
	}
}

func getCatalogStatus(client dynamic.Interface, namespace, catalogName string) ([]ResourceStatus, error) {
	gvr := schema.GroupVersionResource{
		Group:    "ducklake.featherman.dev",
		Version:  viper.GetString("api_version"),
		Resource: "ducklakecatalogs",
	}

	ctx := context.Background()
	resourceClient := client.Resource(gvr).Namespace(namespace)

	var catalogs []unstructured.Unstructured

	if catalogName != "" {
		// Get specific catalog
		catalog, err := resourceClient.Get(ctx, catalogName, metav1.GetOptions{})
		if err != nil {
			if errors.IsNotFound(err) {
				return nil, fmt.Errorf("catalog '%s' not found", catalogName)
			}
			return nil, err
		}
		catalogs = append(catalogs, *catalog)
	} else {
		// List all catalogs
		catalogList, err := resourceClient.List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, err
		}
		catalogs = catalogList.Items
	}

	var resources []ResourceStatus
	for _, catalog := range catalogs {
		status := extractResourceStatus(&catalog, "DuckLakeCatalog")
		resources = append(resources, status)
	}

	return resources, nil
}

func getTableStatus(client dynamic.Interface, namespace, catalogName, tableName string) ([]ResourceStatus, error) {
	gvr := schema.GroupVersionResource{
		Group:    "ducklake.featherman.dev",
		Version:  viper.GetString("api_version"),
		Resource: "ducklaketables",
	}

	ctx := context.Background()
	resourceClient := client.Resource(gvr).Namespace(namespace)

	var tables []unstructured.Unstructured

	if tableName != "" && catalogName != "" {
		// Get specific table
		table, err := resourceClient.Get(ctx, tableName, metav1.GetOptions{})
		if err != nil {
			if errors.IsNotFound(err) {
				return nil, fmt.Errorf("table '%s' not found", tableName)
			}
			return nil, err
		}

		// Check if table belongs to the specified catalog
		if spec, found, _ := unstructured.NestedMap(table.Object, "spec"); found {
			if catalog, found := spec["catalog"]; found && catalog != catalogName {
				return nil, fmt.Errorf("table '%s' does not belong to catalog '%s'", tableName, catalogName)
			}
		}

		tables = append(tables, *table)
	} else {
		// List all tables (optionally filtered by catalog)
		tableList, err := resourceClient.List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, err
		}

		for _, table := range tableList.Items {
			if catalogName != "" {
				// Filter by catalog
				if spec, found, _ := unstructured.NestedMap(table.Object, "spec"); found {
					if catalog, found := spec["catalog"]; found && catalog == catalogName {
						tables = append(tables, table)
					}
				}
			} else {
				tables = append(tables, table)
			}
		}
	}

	var resources []ResourceStatus
	for _, table := range tables {
		status := extractResourceStatus(&table, "DuckLakeTable")
		resources = append(resources, status)
	}

	return resources, nil
}

func getPoolStatus(client dynamic.Interface, namespace, poolName string) ([]ResourceStatus, error) {
	gvr := schema.GroupVersionResource{
		Group:    "ducklake.featherman.dev",
		Version:  viper.GetString("api_version"),
		Resource: "ducklakepools",
	}

	ctx := context.Background()
	resourceClient := client.Resource(gvr).Namespace(namespace)

	var pools []unstructured.Unstructured

	if poolName != "" {
		// Get specific pool
		pool, err := resourceClient.Get(ctx, poolName, metav1.GetOptions{})
		if err != nil {
			if errors.IsNotFound(err) {
				return nil, fmt.Errorf("pool '%s' not found", poolName)
			}
			return nil, err
		}
		pools = append(pools, *pool)
	} else {
		// List all pools
		poolList, err := resourceClient.List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, err
		}
		pools = poolList.Items
	}

	var resources []ResourceStatus
	for _, pool := range pools {
		status := extractResourceStatus(&pool, "DuckLakePool")
		resources = append(resources, status)
	}

	return resources, nil
}

func extractResourceStatus(obj *unstructured.Unstructured, kind string) ResourceStatus {
	resource := ResourceStatus{
		Kind:      kind,
		Name:      obj.GetName(),
		Namespace: obj.GetNamespace(),
		Status:    "Unknown",
		Ready:     "Unknown",
		Age:       formatAge(obj.GetCreationTimestamp().Time),
		Message:   "",
	}

	// Extract status information
	if status, found, _ := unstructured.NestedMap(obj.Object, "status"); found {
		// Check conditions
		if conditions, found, _ := unstructured.NestedSlice(status, "conditions"); found {
			for _, condition := range conditions {
				if condMap, ok := condition.(map[string]interface{}); ok {
					condType, _ := condMap["type"].(string)
					condStatus, _ := condMap["status"].(string)
					message, _ := condMap["message"].(string)

					if condType == "Ready" {
						resource.Ready = condStatus
						if condStatus == "True" {
							resource.Status = "Ready"
						} else {
							resource.Status = "Not Ready"
							resource.Message = message
						}
					}
				}
			}
		}

		// Check phase
		if phase, found, _ := unstructured.NestedString(status, "phase"); found {
			resource.Status = phase
		}
	}

	return resource
}

func formatAge(creationTime time.Time) string {
	age := time.Since(creationTime)

	days := int(age.Hours() / 24)
	hours := int(age.Hours()) % 24
	minutes := int(age.Minutes()) % 60

	if days > 0 {
		return fmt.Sprintf("%dd%dh", days, hours)
	} else if hours > 0 {
		return fmt.Sprintf("%dh%dm", hours, minutes)
	} else {
		return fmt.Sprintf("%dm", minutes)
	}
}

func getKubernetesDynamicClient() (dynamic.Interface, error) {
	kubeconfig := viper.GetString("kubeconfig")
	if kubeconfig == "" {
		kubeconfig = filepath.Join(os.Getenv("HOME"), ".kube", "config")
	}

	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("failed to build kubeconfig: %w", err)
	}

	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create dynamic client: %w", err)
	}

	return dynamicClient, nil
}
