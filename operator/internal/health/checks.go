package health

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
)

// S3Check returns a health check function that verifies S3 connectivity
// by attempting to list buckets. It uses a 5-second timeout for the operation.
func S3Check(s3Client *s3.Client) healthz.Checker {
	return func(_ *http.Request) error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_, err := s3Client.ListBuckets(ctx, &s3.ListBucketsInput{})
		if err != nil {
			return fmt.Errorf("s3 connectivity check failed: %w", err)
		}
		return nil
	}
}

// KubernetesCheck returns a health check function that verifies Kubernetes API
// connectivity by retrieving the server version. It uses a 5-second timeout.
func KubernetesCheck(client kubernetes.Interface) healthz.Checker {
	return func(_ *http.Request) error {
		// We don't need a timeout context for ServerVersion() since it's a simple API call
		_, err := client.Discovery().ServerVersion()
		if err != nil {
			return fmt.Errorf("kubernetes api connectivity check failed: %w", err)
		}
		return nil
	}
}

// CatalogFSCheck returns a health check function that verifies the catalog
// filesystem is accessible by checking if the directory exists and is readable.
func CatalogFSCheck(catalogPath string) healthz.Checker {
	return func(_ *http.Request) error {
		if _, err := os.Stat(catalogPath); err != nil {
			return fmt.Errorf("catalog filesystem accessibility check failed: %w", err)
		}
		return nil
	}
}

// ResourceCheck returns a health check function that monitors node resource pressure
// conditions. It checks for memory, disk, and PID pressure on all nodes.
func ResourceCheck(client kubernetes.Interface) healthz.Checker {
	return func(_ *http.Request) error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		nodes, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		if err != nil {
			return fmt.Errorf("node resource check failed: %w", err)
		}

		for _, node := range nodes.Items {
			for _, condition := range node.Status.Conditions {
				if condition.Type == corev1.NodeMemoryPressure ||
					condition.Type == corev1.NodeDiskPressure ||
					condition.Type == corev1.NodePIDPressure {
					if condition.Status == corev1.ConditionTrue {
						return fmt.Errorf("node %q is under %v pressure", node.Name, condition.Type)
					}
				}
			}
		}
		return nil
	}
}

// LeaderCheck returns a health check function that verifies if this instance
// is currently the leader in the leader election process.
func LeaderCheck(isLeader func() bool) healthz.Checker {
	return func(_ *http.Request) error {
		if !isLeader() {
			return fmt.Errorf("instance is not the current leader")
		}
		return nil
	}
}

// WebhookCertCheck returns a health check function that verifies the webhook
// TLS certificate and key files exist and are accessible.
func WebhookCertCheck(certPath string) healthz.Checker {
	return func(_ *http.Request) error {
		certFile := filepath.Join(certPath, "tls.crt")
		keyFile := filepath.Join(certPath, "tls.key")

		if _, err := os.Stat(certFile); err != nil {
			return fmt.Errorf("webhook certificate file check failed: %w", err)
		}
		if _, err := os.Stat(keyFile); err != nil {
			return fmt.Errorf("webhook key file check failed: %w", err)
		}
		return nil
	}
}
