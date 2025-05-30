package controller

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ducklakev1alpha1 "github.com/TFMV/featherman/operator/api/v1alpha1"
	"github.com/TFMV/featherman/operator/internal/storage"
	"github.com/rs/zerolog"
)

// mockS3Client is a mock implementation of the S3 client interface
type mockS3Client struct{}

func (m *mockS3Client) ListBuckets(ctx context.Context) ([]string, error) {
	return []string{"test-bucket"}, nil
}

func (m *mockS3Client) CreateBucket(ctx context.Context, bucket string) error {
	return nil
}

func (m *mockS3Client) DeleteBucket(ctx context.Context, bucket string) error {
	return nil
}

func (m *mockS3Client) ListObjects(ctx context.Context, bucket, prefix string) ([]string, error) {
	return []string{}, nil
}

func (m *mockS3Client) GetObject(ctx context.Context, bucket, key string) ([]byte, error) {
	return []byte{}, nil
}

func (m *mockS3Client) PutObject(ctx context.Context, bucket, key string, data []byte) error {
	return nil
}

func (m *mockS3Client) DeleteObject(ctx context.Context, bucket, key string) error {
	return nil
}

var _ storage.ObjectStore = &mockS3Client{}

var _ = Describe("DuckLakeCatalog Controller", func() {
	Context("When reconciling a resource", func() {
		const resourceName = "test-resource"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default",
		}
		ducklakecatalog := &ducklakev1alpha1.DuckLakeCatalog{}

		BeforeEach(func() {
			By("creating the storage class")
			storageClass := &storagev1.StorageClass{
				ObjectMeta: metav1.ObjectMeta{
					Name: "standard",
				},
				Provisioner: "kubernetes.io/no-provisioner",
			}
			err := k8sClient.Create(ctx, storageClass)
			if err != nil && !errors.IsAlreadyExists(err) {
				Expect(err).NotTo(HaveOccurred())
			}

			By("creating the MinIO credentials secret")
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "minio-creds",
					Namespace: "default",
				},
				Type: corev1.SecretTypeOpaque,
				Data: map[string][]byte{
					"access-key": []byte("minioadmin"),
					"secret-key": []byte("minioadmin"),
				},
			}
			err = k8sClient.Create(ctx, secret)
			if err != nil && !errors.IsAlreadyExists(err) {
				Expect(err).NotTo(HaveOccurred())
			}

			By("creating the custom resource for the Kind DuckLakeCatalog")
			err = k8sClient.Get(ctx, typeNamespacedName, ducklakecatalog)
			if err != nil && errors.IsNotFound(err) {
				resource := &ducklakev1alpha1.DuckLakeCatalog{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: "default",
					},
					Spec: ducklakev1alpha1.DuckLakeCatalogSpec{
						StorageClass: "standard",
						Size:         "1Gi",
						ObjectStore: ducklakev1alpha1.ObjectStoreSpec{
							Endpoint: "http://minio.minio-test:9000",
							Bucket:   "test-bucket",
							CredentialsSecret: ducklakev1alpha1.SecretReference{
								Name: "minio-creds",
							},
						},
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			resource := &ducklakev1alpha1.DuckLakeCatalog{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance DuckLakeCatalog")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())

			By("Cleanup the storage class")
			storageClass := &storagev1.StorageClass{
				ObjectMeta: metav1.ObjectMeta{
					Name: "standard",
				},
			}
			_ = k8sClient.Delete(ctx, storageClass)

			By("Cleanup the MinIO credentials secret")
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "minio-creds",
					Namespace: "default",
				},
			}
			_ = k8sClient.Delete(ctx, secret)
		})

		It("should successfully reconcile the resource", func() {
			By("Reconciling the created resource")
			log := zerolog.New(zerolog.NewConsoleWriter()).With().Timestamp().Logger()
			s3Client := &mockS3Client{}
			controllerReconciler := &DuckLakeCatalogReconciler{
				Client:  k8sClient,
				Scheme:  k8sClient.Scheme(),
				Logger:  &log,
				Storage: s3Client,
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
		})
	})
})
