package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	initWithMinio    bool
	initOperatorOnly bool
	initTimeout      time.Duration
)

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Initialize a Featherman environment in Kubernetes",
	Long: `Initialize a Featherman data lake environment in the specified Kubernetes namespace.

This command sets up the necessary resources for Featherman to operate, including:
  • Creates the target namespace if it doesn't exist
  • Deploys the Featherman operator (if not present)
  • Optionally deploys MinIO for development environments
  • Sets up required RBAC permissions
  • Creates default configuration

Examples:
  # Initialize Featherman in the default namespace
  feathermanctl init
  
  # Initialize with MinIO for development
  feathermanctl init --with-minio
  
  # Initialize in a specific namespace
  feathermanctl init -n featherman-system --with-minio
  
  # Only install operator, skip other components
  feathermanctl init --operator-only`,
	RunE: func(cmd *cobra.Command, args []string) error {
		namespace := viper.GetString("namespace")

		Info("Initializing Featherman environment",
			zap.String("namespace", namespace),
			zap.Bool("with_minio", initWithMinio),
			zap.Bool("operator_only", initOperatorOnly))

		// Create Kubernetes client
		clientset, err := getKubernetesClientset()
		if err != nil {
			return fmt.Errorf("failed to create Kubernetes client: %w", err)
		}

		// Create namespace if it doesn't exist
		if err := ensureNamespace(clientset, namespace); err != nil {
			return fmt.Errorf("failed to ensure namespace: %w", err)
		}

		// Initialize components
		components := []InitComponent{
			&NamespaceComponent{},
			&OperatorComponent{},
		}

		if initWithMinio && !initOperatorOnly {
			components = append(components, &MinioComponent{})
		}

		if !initOperatorOnly {
			components = append(components, &ConfigComponent{})
		}

		// Execute initialization steps
		ctx, cancel := context.WithTimeout(context.Background(), initTimeout)
		defer cancel()

		for _, component := range components {
			if err := component.Initialize(ctx, clientset, namespace); err != nil {
				return fmt.Errorf("failed to initialize %s: %w", component.Name(), err)
			}
			outputter.PrintSuccess(fmt.Sprintf("Initialized %s", component.Name()))
		}

		outputter.PrintSuccess(fmt.Sprintf("Featherman environment initialized in namespace '%s'", namespace))

		// Display status
		return displayInitStatus(clientset, namespace)
	},
}

func init() {
	rootCmd.AddCommand(initCmd)

	initCmd.Flags().BoolVar(&initWithMinio, "with-minio", false,
		"install MinIO for development environments")
	initCmd.Flags().BoolVar(&initOperatorOnly, "operator-only", false,
		"only install the Featherman operator")
	initCmd.Flags().DurationVar(&initTimeout, "timeout", 10*time.Minute,
		"timeout for initialization operations")

	viper.BindPFlag("with-minio", initCmd.Flags().Lookup("with-minio"))
}

// InitComponent represents a component that can be initialized
type InitComponent interface {
	Name() string
	Initialize(ctx context.Context, clientset kubernetes.Interface, namespace string) error
}

// NamespaceComponent ensures namespace exists
type NamespaceComponent struct{}

func (c *NamespaceComponent) Name() string { return "namespace" }

func (c *NamespaceComponent) Initialize(ctx context.Context, clientset kubernetes.Interface, namespace string) error {
	return ensureNamespace(clientset, namespace)
}

// OperatorComponent installs the Featherman operator
type OperatorComponent struct{}

func (c *OperatorComponent) Name() string { return "operator" }

func (c *OperatorComponent) Initialize(ctx context.Context, clientset kubernetes.Interface, namespace string) error {
	// Check if operator is already running
	deployments := clientset.AppsV1().Deployments(namespace)
	_, err := deployments.Get(ctx, "featherman-operator", metav1.GetOptions{})
	if err == nil {
		Info("Featherman operator already exists", zap.String("namespace", namespace))
		return nil
	}

	if !errors.IsNotFound(err) {
		return fmt.Errorf("failed to check operator deployment: %w", err)
	}

	// Create operator deployment
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "featherman-operator",
			Namespace: namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":    "featherman-operator",
				"app.kubernetes.io/part-of": "featherman",
			},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: int32Ptr(1),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app.kubernetes.io/name": "featherman-operator",
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app.kubernetes.io/name": "featherman-operator",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "operator",
							Image: "ghcr.io/tfmv/featherman-operator:latest",
							Ports: []corev1.ContainerPort{
								{
									ContainerPort: 8080,
									Name:          "metrics",
								},
							},
							Env: []corev1.EnvVar{
								{
									Name: "WATCH_NAMESPACE",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{
											FieldPath: "metadata.namespace",
										},
									},
								},
							},
							Resources: corev1.ResourceRequirements{
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("500m"),
									corev1.ResourceMemory: resource.MustParse("512Mi"),
								},
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("100m"),
									corev1.ResourceMemory: resource.MustParse("128Mi"),
								},
							},
						},
					},
				},
			},
		},
	}

	_, err = deployments.Create(ctx, deployment, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("failed to create operator deployment: %w", err)
	}

	Info("Created Featherman operator deployment", zap.String("namespace", namespace))
	return nil
}

// MinioComponent installs MinIO for development
type MinioComponent struct{}

func (c *MinioComponent) Name() string { return "minio" }

func (c *MinioComponent) Initialize(ctx context.Context, clientset kubernetes.Interface, namespace string) error {
	// Create MinIO deployment
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "minio",
			Namespace: namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":    "minio",
				"app.kubernetes.io/part-of": "featherman",
			},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: int32Ptr(1),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app.kubernetes.io/name": "minio",
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app.kubernetes.io/name": "minio",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "minio",
							Image: "minio/minio:latest",
							Args:  []string{"server", "/data", "--console-address", ":9001"},
							Ports: []corev1.ContainerPort{
								{ContainerPort: 9000, Name: "api"},
								{ContainerPort: 9001, Name: "console"},
							},
							Env: []corev1.EnvVar{
								{Name: "MINIO_ACCESS_KEY", Value: "minioadmin"},
								{Name: "MINIO_SECRET_KEY", Value: "minioadmin"},
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "data",
									MountPath: "/data",
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "data",
							VolumeSource: corev1.VolumeSource{
								EmptyDir: &corev1.EmptyDirVolumeSource{},
							},
						},
					},
				},
			},
		},
	}

	deployments := clientset.AppsV1().Deployments(namespace)
	_, err := deployments.Create(ctx, deployment, metav1.CreateOptions{})
	if err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create MinIO deployment: %w", err)
	}

	// Create MinIO service
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "minio",
			Namespace: namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":    "minio",
				"app.kubernetes.io/part-of": "featherman",
			},
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{
				"app.kubernetes.io/name": "minio",
			},
			Ports: []corev1.ServicePort{
				{
					Name:       "api",
					Port:       9000,
					TargetPort: intstr.FromInt(9000),
				},
				{
					Name:       "console",
					Port:       9001,
					TargetPort: intstr.FromInt(9001),
				},
			},
		},
	}

	services := clientset.CoreV1().Services(namespace)
	_, err = services.Create(ctx, service, metav1.CreateOptions{})
	if err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create MinIO service: %w", err)
	}

	Info("Created MinIO deployment and service", zap.String("namespace", namespace))
	return nil
}

// ConfigComponent creates default configuration
type ConfigComponent struct{}

func (c *ConfigComponent) Name() string { return "configuration" }

func (c *ConfigComponent) Initialize(ctx context.Context, clientset kubernetes.Interface, namespace string) error {
	// Create default configuration ConfigMap
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "featherman-config",
			Namespace: namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":    "featherman-config",
				"app.kubernetes.io/part-of": "featherman",
			},
		},
		Data: map[string]string{
			"config.yaml": `apiVersion: v1
kind: Config
metadata:
  name: default
spec:
  storage:
    type: s3
    endpoint: http://minio:9000
    bucket: featherman
    accessKey: minioadmin
    secretKey: minioadmin
  query:
    endpoint: http://featherman-query:8080
    timeout: 30s
`,
		},
	}

	configMaps := clientset.CoreV1().ConfigMaps(namespace)
	_, err := configMaps.Create(ctx, configMap, metav1.CreateOptions{})
	if err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create configuration: %w", err)
	}

	Info("Created default configuration", zap.String("namespace", namespace))
	return nil
}

// Helper functions
func getKubernetesClientset() (kubernetes.Interface, error) {
	kubeconfig := viper.GetString("kubeconfig")
	if kubeconfig == "" {
		kubeconfig = filepath.Join(os.Getenv("HOME"), ".kube", "config")
	}

	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("failed to build kubeconfig: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create clientset: %w", err)
	}

	return clientset, nil
}

func ensureNamespace(clientset kubernetes.Interface, namespace string) error {
	ctx := context.Background()
	namespaces := clientset.CoreV1().Namespaces()

	_, err := namespaces.Get(ctx, namespace, metav1.GetOptions{})
	if err == nil {
		Info("Namespace already exists", zap.String("namespace", namespace))
		return nil
	}

	if !errors.IsNotFound(err) {
		return fmt.Errorf("failed to check namespace: %w", err)
	}

	// Create namespace
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: namespace,
			Labels: map[string]string{
				"app.kubernetes.io/managed-by": "feathermanctl",
			},
		},
	}

	_, err = namespaces.Create(ctx, ns, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("failed to create namespace: %w", err)
	}

	Info("Created namespace", zap.String("namespace", namespace))
	return nil
}

func displayInitStatus(clientset kubernetes.Interface, namespace string) error {
	ctx := context.Background()

	// Get deployments
	deployments, err := clientset.AppsV1().Deployments(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app.kubernetes.io/part-of=featherman",
	})
	if err != nil {
		return fmt.Errorf("failed to list deployments: %w", err)
	}

	headers := []string{"Component", "Status", "Ready", "Available"}
	var rows [][]string

	for _, deployment := range deployments.Items {
		status := "Unknown"
		if deployment.Status.Conditions != nil {
			for _, condition := range deployment.Status.Conditions {
				if condition.Type == appsv1.DeploymentAvailable {
					if condition.Status == corev1.ConditionTrue {
						status = "Ready"
					} else {
						status = "Not Ready"
					}
					break
				}
			}
		}

		ready := fmt.Sprintf("%d/%d", deployment.Status.ReadyReplicas, deployment.Status.Replicas)
		available := fmt.Sprintf("%d", deployment.Status.AvailableReplicas)

		rows = append(rows, []string{
			deployment.Name,
			status,
			ready,
			available,
		})
	}

	if len(rows) == 0 {
		outputter.PrintWarning("No Featherman components found")
		return nil
	}

	return outputter.PrintTable(headers, rows)
}

func int32Ptr(i int32) *int32 {
	return &i
}
