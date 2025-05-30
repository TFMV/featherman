package e2e

import (
	"context"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	ducklakev1alpha1 "github.com/TFMV/featherman/operator/api/v1alpha1"
	"github.com/TFMV/featherman/operator/test/e2e/setup"
)

var _ = Describe("DuckLakeCatalog Controller", func() {
	const (
		CatalogName      = "test-catalog"
		CatalogNamespace = "default"
	)

	BeforeEach(func() {
		// Copy MinIO credentials secret to default namespace
		ctx := context.Background()
		minioSecret := &corev1.Secret{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{
			Name:      "minio-creds",
			Namespace: setup.MinioNamespace,
		}, minioSecret)).Should(Succeed())

		defaultSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "minio-creds",
				Namespace: CatalogNamespace,
			},
			Type: minioSecret.Type,
			Data: minioSecret.Data,
		}
		err := k8sClient.Create(ctx, defaultSecret)
		if err != nil {
			// If the secret already exists, update it
			Expect(k8sClient.Update(ctx, defaultSecret)).Should(Succeed())
		}
	})

	AfterEach(func() {
		// Clean up the copied secret
		ctx := context.Background()
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "minio-creds",
				Namespace: CatalogNamespace,
			},
		}
		_ = k8sClient.Delete(ctx, secret)
	})

	Context("When creating a DuckLakeCatalog", func() {
		It("Should create and configure the catalog successfully", func() {
			ctx := context.Background()

			By("Creating a new DuckLakeCatalog")
			catalog := &ducklakev1alpha1.DuckLakeCatalog{
				ObjectMeta: metav1.ObjectMeta{
					Name:      CatalogName,
					Namespace: CatalogNamespace,
				},
				Spec: ducklakev1alpha1.DuckLakeCatalogSpec{
					StorageClass: "standard",
					Size:         "1Gi",
					ObjectStore: ducklakev1alpha1.ObjectStoreSpec{
						Endpoint: fmt.Sprintf("http://%s.%s.svc.cluster.local:%d", setup.MinioName, setup.MinioNamespace, setup.MinioPort),
						Bucket:   "test-bucket",
						CredentialsSecret: ducklakev1alpha1.SecretReference{
							Name: "minio-creds",
						},
					},
				},
			}

			Expect(k8sClient.Create(ctx, catalog)).Should(Succeed())

			By("Waiting for the catalog to be ready")
			catalogLookupKey := types.NamespacedName{Name: CatalogName, Namespace: CatalogNamespace}
			createdCatalog := &ducklakev1alpha1.DuckLakeCatalog{}

			Eventually(func() bool {
				err := k8sClient.Get(ctx, catalogLookupKey, createdCatalog)
				if err != nil {
					return false
				}
				return createdCatalog.Status.Phase == ducklakev1alpha1.CatalogPhaseSucceeded
			}, timeout, interval).Should(BeTrue())

			By("Verifying PVC was created")
			pvc := &corev1.PersistentVolumeClaim{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name:      CatalogName + "-catalog",
				Namespace: CatalogNamespace,
			}, pvc)).Should(Succeed())

			By("Verifying PVC specifications")
			Expect(pvc.Spec.AccessModes).Should(ContainElement(corev1.ReadWriteOnce))
			Expect(pvc.Spec.Resources.Requests[corev1.ResourceStorage]).Should(Equal(resource.MustParse("1Gi")))

			By("Adding backup policy")
			updatedCatalog := createdCatalog.DeepCopy()
			updatedCatalog.Spec.BackupPolicy = &ducklakev1alpha1.BackupPolicySpec{
				Schedule:      "0 0 * * *",
				RetentionDays: 7,
			}

			Expect(k8sClient.Update(ctx, updatedCatalog)).Should(Succeed())

			By("Verifying backup policy was updated")
			Eventually(func() int {
				err := k8sClient.Get(ctx, catalogLookupKey, createdCatalog)
				if err != nil {
					return 0
				}
				if createdCatalog.Spec.BackupPolicy == nil {
					return 0
				}
				return createdCatalog.Spec.BackupPolicy.RetentionDays
			}, timeout, interval).Should(Equal(7))

			By("Verifying catalog conditions")
			Eventually(func() bool {
				err := k8sClient.Get(ctx, catalogLookupKey, createdCatalog)
				if err != nil {
					return false
				}
				for _, condition := range createdCatalog.Status.Conditions {
					if condition.Type == "Ready" && condition.Status == metav1.ConditionTrue {
						return true
					}
				}
				return false
			}, timeout, interval).Should(BeTrue())

			By("Cleaning up")
			Expect(k8sClient.Delete(ctx, catalog)).Should(Succeed())

			By("Verifying cleanup")
			Eventually(func() error {
				return k8sClient.Get(ctx, catalogLookupKey, createdCatalog)
			}, timeout, interval).Should(MatchError(ContainSubstring("not found")))
		})
	})

	Context("When handling errors", func() {
		It("Should handle invalid configurations", func() {
			ctx := context.Background()

			By("Creating a catalog with invalid storage class")
			invalidCatalog := &ducklakev1alpha1.DuckLakeCatalog{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "invalid-catalog",
					Namespace: CatalogNamespace,
				},
				Spec: ducklakev1alpha1.DuckLakeCatalogSpec{
					StorageClass: "nonexistent-storage-class",
					Size:         "1Gi",
					ObjectStore: ducklakev1alpha1.ObjectStoreSpec{
						Endpoint: fmt.Sprintf("http://%s.%s.svc.cluster.local:%d", setup.MinioName, setup.MinioNamespace, setup.MinioPort),
						Bucket:   "test-bucket",
						CredentialsSecret: ducklakev1alpha1.SecretReference{
							Name: "minio-creds",
						},
					},
				},
			}

			Expect(k8sClient.Create(ctx, invalidCatalog)).Should(Succeed())

			By("Verifying the catalog enters failed state")
			Eventually(func() ducklakev1alpha1.CatalogPhase {
				catalog := &ducklakev1alpha1.DuckLakeCatalog{}
				err := k8sClient.Get(ctx, types.NamespacedName{
					Name:      "invalid-catalog",
					Namespace: CatalogNamespace,
				}, catalog)
				if err != nil {
					return ""
				}
				return catalog.Status.Phase
			}, timeout, interval).Should(Equal(ducklakev1alpha1.CatalogPhaseFailed))

			By("Cleaning up")
			Expect(k8sClient.Delete(ctx, invalidCatalog)).Should(Succeed())
		})
	})
})
