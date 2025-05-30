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
	"k8s.io/client-go/kubernetes"

	ducklakev1alpha1 "github.com/TFMV/featherman/operator/api/v1alpha1"
	"github.com/TFMV/featherman/operator/internal/duckdb"
	"github.com/TFMV/featherman/operator/internal/sql"
	"github.com/rs/zerolog"
)

var _ = Describe("DuckLakeTable Controller", func() {
	Context("When reconciling a resource", func() {
		const resourceName = "test-resource"
		const catalogName = "test-catalog"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default",
		}
		ducklaketable := &ducklakev1alpha1.DuckLakeTable{}

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

			By("creating the catalog")
			catalog := &ducklakev1alpha1.DuckLakeCatalog{
				ObjectMeta: metav1.ObjectMeta{
					Name:      catalogName,
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
			err = k8sClient.Create(ctx, catalog)
			if err != nil && !errors.IsAlreadyExists(err) {
				Expect(err).NotTo(HaveOccurred())
			}

			By("creating the custom resource for the Kind DuckLakeTable")
			err = k8sClient.Get(ctx, typeNamespacedName, ducklaketable)
			if err != nil && errors.IsNotFound(err) {
				resource := &ducklakev1alpha1.DuckLakeTable{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: "default",
					},
					Spec: ducklakev1alpha1.DuckLakeTableSpec{
						Name:       "test_table",
						CatalogRef: catalogName,
						Columns: []ducklakev1alpha1.ColumnDefinition{
							{
								Name: "id",
								Type: ducklakev1alpha1.SQLTypeInteger,
							},
							{
								Name: "name",
								Type: ducklakev1alpha1.SQLTypeVarChar,
							},
						},
						Format: ducklakev1alpha1.ParquetFormat{
							Compression: ducklakev1alpha1.CompressionZSTD,
						},
						Location: "test-data/test-table",
						Mode:     ducklakev1alpha1.TableModeAppend,
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			resource := &ducklakev1alpha1.DuckLakeTable{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance DuckLakeTable")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())

			By("Cleanup the catalog")
			catalog := &ducklakev1alpha1.DuckLakeCatalog{
				ObjectMeta: metav1.ObjectMeta{
					Name:      catalogName,
					Namespace: "default",
				},
			}
			_ = k8sClient.Delete(ctx, catalog)

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
			sqlGen := sql.NewGenerator()

			// Create Kubernetes client for job manager
			config := testEnv.Config
			k8sClientset, err := kubernetes.NewForConfig(config)
			Expect(err).NotTo(HaveOccurred())
			jobManager := duckdb.NewJobManager(k8sClientset)

			controllerReconciler := &DuckLakeTableReconciler{
				Client:     k8sClient,
				Scheme:     k8sClient.Scheme(),
				Logger:     &log,
				SQLGen:     sqlGen,
				JobManager: jobManager,
			}

			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
		})
	})
})
