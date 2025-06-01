package controller

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	feathermanv1alpha1 "github.com/TFMV/featherman/operator/api/v1alpha1"
	"github.com/rs/zerolog"
)

const (
	timeout  = time.Second * 30
	interval = time.Millisecond * 250

	testNamespace = "default"
)

// mockS3Client and its methods are now defined in suite_test.go

var _ = Describe("DuckLakeCatalog Controller", func() {
	var (
		ctx            context.Context
		cancel         context.CancelFunc
		testCount      int // Counter to ensure unique names
		testReconciler *DuckLakeCatalogReconciler
	)

	BeforeEach(func() {
		ctx, cancel = context.WithCancel(context.Background())

		// Initialize logger
		logger := zerolog.New(zerolog.ConsoleWriter{
			Out:        GinkgoWriter,
			TimeFormat: "15:04:05",
		}).With().Timestamp().Logger()

		// Increment test counter
		testCount++

		// Create a fresh reconciler for each test (not using the background one)
		testReconciler = &DuckLakeCatalogReconciler{
			Client:   k8sClient,
			Scheme:   k8sManager.GetScheme(),
			Storage:  mockS3,
			Recorder: record.NewFakeRecorder(100),
			Logger:   &logger,
		}
		// DON'T call SetupWithManager - we want manual reconciliation only

		// Reset mock S3 client error
		mockS3.SetError(nil)

		// Ensure storage class exists before each test
		storageClass := &storagev1.StorageClass{}
		err := k8sClient.Get(ctx, types.NamespacedName{Name: "standard"}, storageClass)
		if errors.IsNotFound(err) {
			// Recreate the storage class if it was deleted
			storageClass = &storagev1.StorageClass{
				ObjectMeta: metav1.ObjectMeta{
					Name: "standard",
				},
				Provisioner: "kubernetes.io/aws-ebs",
			}
			Expect(k8sClient.Create(ctx, storageClass)).Should(Succeed())
		}

		// Ensure minio-creds secret exists before each test
		secret := &corev1.Secret{}
		err = k8sClient.Get(ctx, types.NamespacedName{Name: "minio-creds", Namespace: testNamespace}, secret)
		if errors.IsNotFound(err) {
			// Recreate the secret if it was deleted
			secret = &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "minio-creds",
					Namespace: testNamespace,
				},
				StringData: map[string]string{
					"access-key": "minioadmin",
					"secret-key": "minioadmin",
				},
			}
			Expect(k8sClient.Create(ctx, secret)).Should(Succeed())
		}
	})

	AfterEach(func() {
		// Simple cleanup - just delete resources without complex waiting
		testName := fmt.Sprintf("test-ducklake-%d", testCount)

		// Try to delete catalog
		catalog := &feathermanv1alpha1.DuckLakeCatalog{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, catalog); err == nil {
			// Remove finalizer to allow deletion
			catalog.Finalizers = nil
			_ = k8sClient.Update(ctx, catalog)
			_ = k8sClient.Delete(ctx, catalog)
		}

		// Try to delete PVC
		pvc := &corev1.PersistentVolumeClaim{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: testName + "-catalog", Namespace: testNamespace}, pvc); err == nil {
			_ = k8sClient.Delete(ctx, pvc)
		}

		cancel()
	})

	Context("DuckLakeCatalog Reconciliation", func() {
		It("should successfully reconcile the resource", func() {
			testName := fmt.Sprintf("test-ducklake-%d", testCount)
			catalog := createTestCatalog(testName)
			Expect(k8sClient.Create(ctx, catalog)).Should(Succeed())

			// Wait for resource to be created
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, &feathermanv1alpha1.DuckLakeCatalog{})
			}, timeout, interval).Should(Succeed())

			// Manually trigger reconciliation
			req := reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      testName,
					Namespace: testNamespace,
				},
			}

			// First reconciliation should succeed
			result, err := testReconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(BeZero())

			// Wait for status to be updated
			Eventually(func() feathermanv1alpha1.CatalogPhase {
				updatedCatalog := &feathermanv1alpha1.DuckLakeCatalog{}
				if err := k8sClient.Get(ctx, req.NamespacedName, updatedCatalog); err != nil {
					return ""
				}
				return updatedCatalog.Status.Phase
			}, timeout, interval).Should(Equal(feathermanv1alpha1.CatalogPhaseSucceeded))

			// Verify PVC was created
			Eventually(func() error {
				pvc := &corev1.PersistentVolumeClaim{}
				return k8sClient.Get(ctx, types.NamespacedName{
					Name:      testName + "-catalog",
					Namespace: testNamespace,
				}, pvc)
			}, timeout, interval).Should(Succeed())
		})

		It("should fail when storage class doesn't exist", func() {
			testName := fmt.Sprintf("test-ducklake-%d", testCount)

			// Delete the standard storage class
			storageClass := &storagev1.StorageClass{
				ObjectMeta: metav1.ObjectMeta{
					Name: "standard",
				},
			}
			Expect(k8sClient.Delete(ctx, storageClass)).Should(Succeed())

			// Wait for deletion
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: "standard"}, &storagev1.StorageClass{})
				return errors.IsNotFound(err)
			}, timeout, interval).Should(BeTrue())

			catalog := createTestCatalog(testName)
			Expect(k8sClient.Create(ctx, catalog)).Should(Succeed())

			// Wait for resource to be created
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, &feathermanv1alpha1.DuckLakeCatalog{})
			}, timeout, interval).Should(Succeed())

			// Manually trigger reconciliation
			req := reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      testName,
					Namespace: testNamespace,
				},
			}

			// Reconciliation should fail
			_, err := testReconciler.Reconcile(ctx, req)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("storage class standard not found"))

			// Wait for status to be updated
			Eventually(func() feathermanv1alpha1.CatalogPhase {
				updatedCatalog := &feathermanv1alpha1.DuckLakeCatalog{}
				if err := k8sClient.Get(ctx, req.NamespacedName, updatedCatalog); err != nil {
					return ""
				}
				return updatedCatalog.Status.Phase
			}, timeout, interval).Should(Equal(feathermanv1alpha1.CatalogPhaseFailed))

			// Recreate the storage class for other tests
			storageClass = &storagev1.StorageClass{
				ObjectMeta: metav1.ObjectMeta{
					Name: "standard",
				},
				Provisioner: "kubernetes.io/aws-ebs",
			}
			Expect(k8sClient.Create(ctx, storageClass)).Should(Succeed())
		})

		It("should fail when S3 connection fails", func() {
			testName := fmt.Sprintf("test-ducklake-%d", testCount)

			// Set up mock S3 client to return error
			mockS3.SetError(fmt.Errorf("S3 connection failed"))

			catalog := createTestCatalog(testName)
			Expect(k8sClient.Create(ctx, catalog)).Should(Succeed())

			// Wait for resource to be created
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, &feathermanv1alpha1.DuckLakeCatalog{})
			}, timeout, interval).Should(Succeed())

			// Manually trigger reconciliation
			req := reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      testName,
					Namespace: testNamespace,
				},
			}

			// Reconciliation should fail
			_, err := testReconciler.Reconcile(ctx, req)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to validate S3 connection"))

			// Wait for status to be updated
			Eventually(func() feathermanv1alpha1.CatalogPhase {
				updatedCatalog := &feathermanv1alpha1.DuckLakeCatalog{}
				if err := k8sClient.Get(ctx, req.NamespacedName, updatedCatalog); err != nil {
					return ""
				}
				return updatedCatalog.Status.Phase
			}, timeout, interval).Should(Equal(feathermanv1alpha1.CatalogPhaseFailed))
		})

		It("should handle resource deletion properly", func() {
			testName := fmt.Sprintf("test-ducklake-%d", testCount)
			catalog := createTestCatalog(testName)
			Expect(k8sClient.Create(ctx, catalog)).Should(Succeed())

			// Wait for resource to be created
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, &feathermanv1alpha1.DuckLakeCatalog{})
			}, timeout, interval).Should(Succeed())

			req := reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      testName,
					Namespace: testNamespace,
				},
			}

			// First reconciliation to create resources
			result, err := testReconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(BeZero())

			// Wait for PVC to be created
			Eventually(func() error {
				pvc := &corev1.PersistentVolumeClaim{}
				return k8sClient.Get(ctx, types.NamespacedName{
					Name:      testName + "-catalog",
					Namespace: testNamespace,
				}, pvc)
			}, timeout, interval).Should(Succeed())

			// Delete the catalog
			Expect(k8sClient.Delete(ctx, catalog)).Should(Succeed())

			// Wait for deletion timestamp to be set
			Eventually(func() bool {
				updatedCatalog := &feathermanv1alpha1.DuckLakeCatalog{}
				if err := k8sClient.Get(ctx, req.NamespacedName, updatedCatalog); err != nil {
					return false
				}
				return !updatedCatalog.DeletionTimestamp.IsZero()
			}, timeout, interval).Should(BeTrue())

			// Trigger deletion reconciliation
			result, err = testReconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			// The reconciler should have removed the finalizer
			Eventually(func() []string {
				finalCatalog := &feathermanv1alpha1.DuckLakeCatalog{}
				err := k8sClient.Get(ctx, req.NamespacedName, finalCatalog)
				if err != nil {
					// If the resource is not found, that's what we expect after deletion
					if errors.IsNotFound(err) {
						return []string{}
					}
					// For other errors, return a non-empty slice to continue waiting
					return []string{"error"}
				}
				return finalCatalog.Finalizers
			}, timeout, interval).Should(BeEmpty())

			// In envtest, we can't verify full PVC deletion due to lack of storage provisioner
			// But we can verify the controller tried to delete it
			// The actual deletion would happen in a real cluster
		})
	})
})

func createTestCatalog(name string) *feathermanv1alpha1.DuckLakeCatalog {
	return &feathermanv1alpha1.DuckLakeCatalog{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testNamespace,
		},
		Spec: feathermanv1alpha1.DuckLakeCatalogSpec{
			StorageClass: "standard",
			Size:         "1Gi",
			ObjectStore: feathermanv1alpha1.ObjectStoreSpec{
				Endpoint: "http://minio.minio-test:9000",
				Bucket:   "test-bucket",
				CredentialsSecret: feathermanv1alpha1.SecretReference{
					Name: "minio-creds",
				},
			},
		},
	}
}
