package cmd

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	deployDryRun   bool
	deployValidate bool
	deployWait     bool
)

var deployCmd = &cobra.Command{
	Use:   "deploy [manifest-file]",
	Short: "Deploy a catalog or table manifest to Kubernetes",
	Long: `Deploy Featherman resources from YAML manifest files.

The deploy command reads YAML manifest files containing DuckLakeCatalog,
DuckLakeTable, or DuckLakePool resources and applies them to the Kubernetes cluster.

Examples:
  # Deploy a single manifest file
  feathermanctl deploy catalog.yaml
  
  # Deploy all manifests in a directory
  feathermanctl deploy ./manifests/
  
  # Dry run deployment
  feathermanctl deploy catalog.yaml --dry-run
  
  # Deploy and wait for resources to be ready
  feathermanctl deploy catalog.yaml --wait`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		manifestPath := args[0]

		Info("Starting deployment",
			zap.String("manifest", manifestPath),
			zap.Bool("dry_run", deployDryRun))

		// Load Kubernetes client
		kubeClient, err := getKubernetesClient()
		if err != nil {
			return fmt.Errorf("failed to create Kubernetes client: %w", err)
		}

		// Process manifest(s)
		manifests, err := loadManifests(manifestPath)
		if err != nil {
			return fmt.Errorf("failed to load manifests: %w", err)
		}

		if len(manifests) == 0 {
			outputter.PrintWarning("No manifests found to deploy")
			return nil
		}

		// Deploy each manifest
		var deployed []string
		for _, manifest := range manifests {
			if err := deployManifest(kubeClient, manifest); err != nil {
				Error("Failed to deploy manifest",
					zap.String("kind", manifest.GetKind()),
					zap.String("name", manifest.GetName()),
					zap.Error(err))
				return err
			}
			deployed = append(deployed, fmt.Sprintf("%s/%s", manifest.GetKind(), manifest.GetName()))
		}

		if deployDryRun {
			outputter.PrintInfo(fmt.Sprintf("Dry run completed. Would deploy %d resources", len(deployed)))
		} else {
			outputter.PrintSuccess(fmt.Sprintf("Successfully deployed %d resources", len(deployed)))
		}

		// Display deployed resources
		if len(deployed) > 0 {
			headers := []string{"Resource Type", "Name", "Status"}
			var rows [][]string
			for _, resource := range deployed {
				parts := strings.Split(resource, "/")
				status := "Deployed"
				if deployDryRun {
					status = "Dry Run"
				}
				rows = append(rows, []string{parts[0], parts[1], status})
			}
			return outputter.PrintTable(headers, rows)
		}

		return nil
	},
}

func init() {
	rootCmd.AddCommand(deployCmd)

	deployCmd.Flags().BoolVar(&deployDryRun, "dry-run", false,
		"preview deployment without applying changes")
	deployCmd.Flags().BoolVar(&deployValidate, "validate", true,
		"validate manifests before deployment")
	deployCmd.Flags().BoolVar(&deployWait, "wait", false,
		"wait for resources to be ready")
}

// getKubernetesClient creates a Kubernetes dynamic client
func getKubernetesClient() (dynamic.Interface, error) {
	kubeconfig := viper.GetString("kubeconfig")
	if kubeconfig == "" {
		kubeconfig = filepath.Join(os.Getenv("HOME"), ".kube", "config")
	}

	// Build the kubeconfig
	kubeConfig, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("failed to build kubeconfig: %w", err)
	}

	// Create dynamic client
	dynamicClient, err := dynamic.NewForConfig(kubeConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create dynamic client: %w", err)
	}

	return dynamicClient, nil
}

// loadManifests loads YAML manifests from file or directory
func loadManifests(path string) ([]*unstructured.Unstructured, error) {
	var manifests []*unstructured.Unstructured

	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("failed to stat path %s: %w", path, err)
	}

	if info.IsDir() {
		// Load all YAML files from directory
		files, err := filepath.Glob(filepath.Join(path, "*.yaml"))
		if err != nil {
			return nil, fmt.Errorf("failed to glob YAML files: %w", err)
		}

		yamlFiles, err := filepath.Glob(filepath.Join(path, "*.yml"))
		if err != nil {
			return nil, fmt.Errorf("failed to glob YML files: %w", err)
		}
		files = append(files, yamlFiles...)

		for _, file := range files {
			fileManifests, err := loadManifestFile(file)
			if err != nil {
				return nil, err
			}
			manifests = append(manifests, fileManifests...)
		}
	} else {
		// Load single file
		manifests, err = loadManifestFile(path)
		if err != nil {
			return nil, err
		}
	}

	return manifests, nil
}

// loadManifestFile loads manifests from a single YAML file
func loadManifestFile(filename string) ([]*unstructured.Unstructured, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %w", filename, err)
	}

	// Split YAML documents
	documents := strings.Split(string(data), "\n---\n")
	var manifests []*unstructured.Unstructured

	for i, doc := range documents {
		doc = strings.TrimSpace(doc)
		if doc == "" {
			continue
		}

		// Parse YAML into unstructured object
		var obj map[string]interface{}
		if err := yaml.Unmarshal([]byte(doc), &obj); err != nil {
			return nil, fmt.Errorf("failed to parse YAML document %d in file %s: %w", i+1, filename, err)
		}

		if len(obj) == 0 {
			continue
		}

		manifest := &unstructured.Unstructured{Object: obj}

		// Validate required fields
		if manifest.GetAPIVersion() == "" {
			return nil, fmt.Errorf("missing apiVersion in document %d of file %s", i+1, filename)
		}
		if manifest.GetKind() == "" {
			return nil, fmt.Errorf("missing kind in document %d of file %s", i+1, filename)
		}

		// Validate Featherman resource types
		validKinds := map[string]bool{
			"DuckLakeCatalog": true,
			"DuckLakeTable":   true,
			"DuckLakePool":    true,
		}

		if !validKinds[manifest.GetKind()] {
			Warn("Unknown resource kind",
				zap.String("kind", manifest.GetKind()),
				zap.String("file", filename))
		}

		manifests = append(manifests, manifest)
	}

	return manifests, nil
}

// deployManifest deploys a single manifest to Kubernetes
func deployManifest(client dynamic.Interface, manifest *unstructured.Unstructured) error {
	// Set namespace if not specified
	namespace := manifest.GetNamespace()
	if namespace == "" {
		namespace = viper.GetString("namespace")
		manifest.SetNamespace(namespace)
	}

	// Validate namespace name
	if errs := validation.IsDNS1123Label(namespace); len(errs) > 0 {
		return fmt.Errorf("invalid namespace name: %v", errs)
	}

	// Create GroupVersionResource
	gvr := schema.GroupVersionResource{
		Group:    "ducklake.featherman.dev",
		Version:  viper.GetString("api_version"),
		Resource: strings.ToLower(manifest.GetKind()) + "s",
	}

	ctx := context.Background()
	resourceClient := client.Resource(gvr).Namespace(namespace)

	if deployDryRun {
		Info("Would deploy resource (dry run)",
			zap.String("kind", manifest.GetKind()),
			zap.String("name", manifest.GetName()),
			zap.String("namespace", namespace))
		return nil
	}

	// Check if resource already exists
	existing, err := resourceClient.Get(ctx, manifest.GetName(), metav1.GetOptions{})
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("failed to check existing resource: %w", err)
	}

	if existing != nil {
		// Update existing resource
		Info("Updating existing resource",
			zap.String("kind", manifest.GetKind()),
			zap.String("name", manifest.GetName()),
			zap.String("namespace", namespace))

		manifest.SetResourceVersion(existing.GetResourceVersion())
		_, err = resourceClient.Update(ctx, manifest, metav1.UpdateOptions{})
		if err != nil {
			return fmt.Errorf("failed to update resource: %w", err)
		}
	} else {
		// Create new resource
		Info("Creating new resource",
			zap.String("kind", manifest.GetKind()),
			zap.String("name", manifest.GetName()),
			zap.String("namespace", namespace))

		_, err = resourceClient.Create(ctx, manifest, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("failed to create resource: %w", err)
		}
	}

	return nil
}
