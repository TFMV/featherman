/*
Copyright 2024 Thomas McGeehan.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	stderrors "errors" // Renamed to avoid conflict with k8s.io/apimachinery/pkg/api/errors
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	feathermanv1alpha1 "github.com/TFMV/featherman/operator/api/v1alpha1"
	storage "github.com/TFMV/featherman/operator/internal/storage"
	// +kubebuilder:scaffold:imports
)

// These tests use Ginkgo (BDD-style Go testing framework). Refer to
// http://onsi.github.io/ginkgo/ to learn more about Ginkgo.

var (
	ctx        context.Context
	cancel     context.CancelFunc
	testEnv    *envtest.Environment
	cfg        *rest.Config
	k8sClient  client.Client
	k8sManager ctrl.Manager
	mockS3     *mockS3Client
)

// mockS3Client is a mock implementation of the S3 client interface
type mockS3Client struct {
	shouldFailListBuckets bool
	forceError            error // New field to force specific errors
}

func (m *mockS3Client) SetError(err error) { // New method to set forced error
	m.forceError = err
	m.shouldFailListBuckets = (err != nil) // also ensure shouldFailListBuckets is consistent
}

func (m *mockS3Client) ListBuckets(ctx context.Context) ([]string, error) {
	if m.forceError != nil {
		return nil, m.forceError
	}
	if m.shouldFailListBuckets {
		return nil, stderrors.New("failed to list buckets")
	}
	return []string{"test-bucket"}, nil
}

func (m *mockS3Client) CreateBucket(ctx context.Context, bucket string) error {
	return m.forceError
}

func (m *mockS3Client) DeleteBucket(ctx context.Context, bucket string) error {
	return m.forceError
}

func (m *mockS3Client) ListObjects(ctx context.Context, bucket, prefix string) ([]string, error) {
	if m.forceError != nil {
		return nil, m.forceError
	}
	return []string{}, nil
}

func (m *mockS3Client) GetObject(ctx context.Context, bucket, key string) ([]byte, error) {
	if m.forceError != nil {
		return nil, m.forceError
	}
	return []byte{}, nil
}

func (m *mockS3Client) PutObject(ctx context.Context, bucket, key string, data []byte) error {
	return m.forceError
}

func (m *mockS3Client) DeleteObject(ctx context.Context, bucket, key string) error {
	return m.forceError
}

var _ storage.ObjectStore = &mockS3Client{}

// newMockS3Client creates a new instance of the local mockS3Client
func newMockS3Client() *mockS3Client {
	return &mockS3Client{}
}

func TestAPIs(t *testing.T) {
	RegisterFailHandler(Fail)

	RunSpecs(t, "Controller Suite")
}

var _ = BeforeSuite(func() {
	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))

	ctx, cancel = context.WithCancel(context.TODO())

	var err error
	err = feathermanv1alpha1.AddToScheme(scheme.Scheme)
	Expect(err).NotTo(HaveOccurred())

	// +kubebuilder:scaffold:scheme

	By("bootstrapping test environment")
	testEnv = &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}

	// Retrieve the first found binary directory to allow running tests from IDEs
	if getFirstFoundEnvTestBinaryDir() != "" {
		testEnv.BinaryAssetsDirectory = getFirstFoundEnvTestBinaryDir()
	}

	// cfg is defined in this file globally.
	cfg, err = testEnv.Start()
	Expect(err).NotTo(HaveOccurred())
	Expect(cfg).NotTo(BeNil())

	// Create the controller manager
	k8sManager, err = ctrl.NewManager(cfg, ctrl.Options{
		Scheme: scheme.Scheme,
		// Disable metrics server to avoid port conflicts in tests
		Metrics: metricsserver.Options{
			BindAddress: "0",
		},
	})
	Expect(err).NotTo(HaveOccurred())
	Expect(k8sManager).NotTo(BeNil())

	// Get the client from the manager
	k8sClient = k8sManager.GetClient()
	Expect(k8sClient).NotTo(BeNil())

	// Clean up any lingering resources from previous test runs
	By("cleaning up lingering resources from previous test runs")
	cleanupCtx := context.Background()

	// List and delete all DuckLakeCatalogs
	catalogList := &feathermanv1alpha1.DuckLakeCatalogList{}
	if err := k8sClient.List(cleanupCtx, catalogList); err == nil {
		for _, catalog := range catalogList.Items {
			// Create a copy to avoid pointer issues
			catalogCopy := catalog
			if err := k8sClient.Delete(cleanupCtx, &catalogCopy); err != nil && !errors.IsNotFound(err) {
				// Log but don't fail - just trying to clean up
				logf.Log.Error(err, "Failed to delete lingering catalog", "name", catalog.Name)
			}
		}
		// Wait a bit for deletions to process
		time.Sleep(2 * time.Second)
	}

	// Create test storage class
	storageClass := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "standard",
		},
		Provisioner: "kubernetes.io/aws-ebs", // or a mock provisioner if needed for envtest
	}
	if err := k8sClient.Create(context.Background(), storageClass); err != nil && !errors.IsAlreadyExists(err) {
		Expect(err).NotTo(HaveOccurred())
	}

	// Create S3 credentials secret
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "minio-creds",
			Namespace: "default", // Ensure this matches testNamespace in controller test
		},
		StringData: map[string]string{
			"access-key": "minioadmin",
			"secret-key": "minioadmin",
		},
	}
	if err := k8sClient.Create(context.Background(), secret); err != nil && !errors.IsAlreadyExists(err) {
		Expect(err).NotTo(HaveOccurred())
	}

	// Initialize mock S3 client
	mockS3 = newMockS3Client()

	// DON'T create or start the reconciler here - let individual tests handle it
	// This prevents race conditions between background controller and test reconcilers

	// Start the manager (needed for the cache) but don't add any controllers
	go func() {
		defer GinkgoRecover()
		err = k8sManager.Start(ctx)
		Expect(err).NotTo(HaveOccurred(), "failed to run manager")
	}()

	// Wait for the cache to be ready
	By("waiting for manager cache to sync")
	Eventually(func() bool {
		catalogList := &feathermanv1alpha1.DuckLakeCatalogList{}
		err := k8sClient.List(context.Background(), catalogList)
		return err == nil
	}, 10*time.Second, 100*time.Millisecond).Should(BeTrue())

	// Now clean up any remaining resources
	By("cleanup of any remaining resources")
	cleanupCtx = context.Background()
	finalCatalogList := &feathermanv1alpha1.DuckLakeCatalogList{}
	if err := k8sClient.List(cleanupCtx, finalCatalogList); err == nil {
		for _, catalog := range finalCatalogList.Items {
			// Force delete by removing finalizers first
			catalogCopy := catalog
			if len(catalogCopy.Finalizers) > 0 {
				catalogCopy.Finalizers = nil
				if err := k8sClient.Update(cleanupCtx, &catalogCopy); err == nil {
					// Now delete
					_ = k8sClient.Delete(cleanupCtx, &catalogCopy)
				}
			}
		}
	}

	// Also clean up any leftover PVCs
	pvcList := &corev1.PersistentVolumeClaimList{}
	if err := k8sClient.List(cleanupCtx, pvcList); err == nil {
		for _, pvc := range pvcList.Items {
			// Delete any catalog-related PVCs
			if strings.HasSuffix(pvc.Name, "-catalog") {
				pvcCopy := pvc
				_ = k8sClient.Delete(cleanupCtx, &pvcCopy)
			}
		}
	}

	// Wait a bit for cleanup to complete
	time.Sleep(500 * time.Millisecond)
})

var _ = AfterSuite(func() {
	By("tearing down the test environment")

	// Cancel the context to stop the manager
	cancel()

	// Delete S3 credentials secret
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "minio-creds",
			Namespace: "default",
		},
	}
	err := k8sClient.Delete(context.Background(), secret)
	if err != nil && !errors.IsNotFound(err) {
		Expect(err).NotTo(HaveOccurred())
	}

	// Delete test storage class
	storageClass := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "standard",
		},
	}
	err = k8sClient.Delete(context.Background(), storageClass)
	if err != nil && !errors.IsNotFound(err) {
		Expect(err).NotTo(HaveOccurred())
	}

	err = testEnv.Stop()
	Expect(err).NotTo(HaveOccurred())
})

// getFirstFoundEnvTestBinaryDir locates the first binary in the specified path.
// ENVTEST-based tests depend on specific binaries, usually located in paths set by
// controller-runtime. When running tests directly (e.g., via an IDE) without using
// Makefile targets, the 'BinaryAssetsDirectory' must be explicitly configured.
//
// This function streamlines the process by finding the required binaries, similar to
// setting the 'KUBEBUILDER_ASSETS' environment variable. To ensure the binaries are
// properly set up, run 'make setup-envtest' beforehand.
func getFirstFoundEnvTestBinaryDir() string {
	basePath := filepath.Join("..", "..", "bin", "k8s")
	entries, err := os.ReadDir(basePath)
	if err != nil {
		logf.Log.Error(err, "Failed to read directory", "path", basePath)
		return ""
	}
	for _, entry := range entries {
		if entry.IsDir() {
			return filepath.Join(basePath, entry.Name())
		}
	}
	return ""
}
