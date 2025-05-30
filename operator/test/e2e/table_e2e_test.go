package e2e

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	ducklakev1alpha1 "github.com/TFMV/featherman/operator/api/v1alpha1"
	"github.com/TFMV/featherman/operator/test/e2e/setup"
)

var _ = Describe("DuckLakeTable Controller", func() {
	const (
		CatalogName      = "test-catalog"
		TableName        = "test-table"
		CatalogNamespace = "minio-test"
		timeout          = time.Second * 120
		interval         = time.Second * 1
	)

	BeforeEach(func() {
		// No need to copy MinIO credentials secret since we're using the same namespace
	})

	AfterEach(func() {
		ctx := context.Background()

		// Clean up the table
		table := &ducklakev1alpha1.DuckLakeTable{
			ObjectMeta: metav1.ObjectMeta{
				Name:      TableName,
				Namespace: CatalogNamespace,
			},
		}
		err := k8sClient.Get(ctx, types.NamespacedName{
			Name:      TableName,
			Namespace: CatalogNamespace,
		}, table)
		if err == nil {
			Expect(k8sClient.Delete(ctx, table)).Should(Succeed())
			// Wait for table to be deleted
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{
					Name:      TableName,
					Namespace: CatalogNamespace,
				}, table)
			}, timeout, interval).ShouldNot(Succeed())
		}

		// Clean up the catalog
		catalog := &ducklakev1alpha1.DuckLakeCatalog{
			ObjectMeta: metav1.ObjectMeta{
				Name:      CatalogName,
				Namespace: CatalogNamespace,
			},
		}
		err = k8sClient.Get(ctx, types.NamespacedName{
			Name:      CatalogName,
			Namespace: CatalogNamespace,
		}, catalog)
		if err == nil {
			Expect(k8sClient.Delete(ctx, catalog)).Should(Succeed())
			// Wait for catalog to be deleted
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{
					Name:      CatalogName,
					Namespace: CatalogNamespace,
				}, catalog)
			}, timeout, interval).ShouldNot(Succeed())
		}
	})

	Context("When creating a table and loading data", func() {
		It("Should successfully create, load, and query data", func() {
			ctx := context.Background()

			By("Creating a catalog")
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
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{
					Name:      CatalogName,
					Namespace: CatalogNamespace,
				}, catalog)
				if err != nil {
					return false
				}
				return catalog.Status.Phase == ducklakev1alpha1.CatalogPhaseSucceeded
			}, timeout, interval).Should(BeTrue())

			By("Creating a table")
			table := &ducklakev1alpha1.DuckLakeTable{
				ObjectMeta: metav1.ObjectMeta{
					Name:      TableName,
					Namespace: CatalogNamespace,
				},
				Spec: ducklakev1alpha1.DuckLakeTableSpec{
					CatalogRef: CatalogName,
					Name:       "users",
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
					Location: "test-data/users",
					Mode:     ducklakev1alpha1.TableModeAppend,
				},
			}
			Expect(k8sClient.Create(ctx, table)).Should(Succeed())

			By("Waiting for the table to be ready")
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{
					Name:      TableName,
					Namespace: CatalogNamespace,
				}, table)
				if err != nil {
					return false
				}
				return table.Status.Phase == ducklakev1alpha1.TablePhaseSucceeded
			}, timeout, interval).Should(BeTrue())

			By("Creating a job to insert test data")
			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "insert-test-data-",
					Namespace:    CatalogNamespace,
					Labels: map[string]string{
						"app.kubernetes.io/name":      "featherman",
						"app.kubernetes.io/component": "duckdb",
						"app.kubernetes.io/part-of":   "test",
					},
				},
				Spec: batchv1.JobSpec{
					BackoffLimit: &[]int32{3}[0],
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"app.kubernetes.io/name":      "featherman",
								"app.kubernetes.io/component": "duckdb",
								"app.kubernetes.io/part-of":   "test",
							},
						},
						Spec: corev1.PodSpec{
							RestartPolicy: corev1.RestartPolicyOnFailure,
							SecurityContext: &corev1.PodSecurityContext{
								RunAsNonRoot: &[]bool{true}[0],
								RunAsUser:    &[]int64{1000}[0],
								FSGroup:      &[]int64{1000}[0],
							},
							Containers: []corev1.Container{
								{
									Name:  "duckdb",
									Image: "datacatering/duckdb:v1.3.0",
									Command: []string{
										"/duckdb",
										"/catalog/catalog.db",
										"-c",
										"INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie');",
									},
									Resources: corev1.ResourceRequirements{
										Requests: corev1.ResourceList{
											corev1.ResourceCPU:    resource.MustParse("100m"),
											corev1.ResourceMemory: resource.MustParse("256Mi"),
										},
										Limits: corev1.ResourceList{
											corev1.ResourceCPU:    resource.MustParse("500m"),
											corev1.ResourceMemory: resource.MustParse("1Gi"),
										},
									},
									SecurityContext: &corev1.SecurityContext{
										ReadOnlyRootFilesystem: &[]bool{false}[0],
										Capabilities: &corev1.Capabilities{
											Drop: []corev1.Capability{"ALL"},
										},
									},
									VolumeMounts: []corev1.VolumeMount{
										{
											Name:      "catalog",
											MountPath: "/catalog",
										},
										{
											Name:      "tmp",
											MountPath: "/tmp",
										},
									},
									Env: []corev1.EnvVar{
										{
											Name:  "DUCKDB_S3_ENDPOINT",
											Value: fmt.Sprintf("http://%s.%s.svc.cluster.local:%d", setup.MinioName, setup.MinioNamespace, setup.MinioPort),
										},
										{
											Name: "DUCKDB_S3_ACCESS_KEY_ID",
											ValueFrom: &corev1.EnvVarSource{
												SecretKeyRef: &corev1.SecretKeySelector{
													LocalObjectReference: corev1.LocalObjectReference{
														Name: "minio-creds",
													},
													Key: "access-key",
												},
											},
										},
										{
											Name: "DUCKDB_S3_SECRET_ACCESS_KEY",
											ValueFrom: &corev1.EnvVarSource{
												SecretKeyRef: &corev1.SecretKeySelector{
													LocalObjectReference: corev1.LocalObjectReference{
														Name: "minio-creds",
													},
													Key: "secret-key",
												},
											},
										},
										{
											Name:  "DUCKDB_MEMORY_LIMIT",
											Value: "768MB",
										},
										{
											Name:  "DUCKDB_THREADS",
											Value: "2",
										},
									},
								},
							},
							Volumes: []corev1.Volume{
								{
									Name: "catalog",
									VolumeSource: corev1.VolumeSource{
										PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
											ClaimName: fmt.Sprintf("%s-catalog", CatalogName),
										},
									},
								},
								{
									Name: "tmp",
									VolumeSource: corev1.VolumeSource{
										EmptyDir: &corev1.EmptyDirVolumeSource{},
									},
								},
							},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, job)).Should(Succeed())

			By("Waiting for the insert job to complete")
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{
					Name:      job.Name,
					Namespace: job.Namespace,
				}, job)
				if err != nil {
					return false
				}
				return job.Status.Succeeded > 0
			}, timeout, interval).Should(BeTrue())

			By("Creating a job to query the data")
			queryJob := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "query-test-data-",
					Namespace:    CatalogNamespace,
					Labels: map[string]string{
						"app.kubernetes.io/name":      "featherman",
						"app.kubernetes.io/component": "duckdb",
						"app.kubernetes.io/part-of":   "test",
					},
				},
				Spec: batchv1.JobSpec{
					BackoffLimit: &[]int32{3}[0],
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"app.kubernetes.io/name":      "featherman",
								"app.kubernetes.io/component": "duckdb",
								"app.kubernetes.io/part-of":   "test",
							},
						},
						Spec: corev1.PodSpec{
							RestartPolicy: corev1.RestartPolicyOnFailure,
							SecurityContext: &corev1.PodSecurityContext{
								RunAsNonRoot: &[]bool{true}[0],
								RunAsUser:    &[]int64{1000}[0],
								FSGroup:      &[]int64{1000}[0],
							},
							Containers: []corev1.Container{
								{
									Name:  "duckdb",
									Image: "datacatering/duckdb:v1.3.0",
									Command: []string{
										"/duckdb",
										"/catalog/catalog.db",
										"-c",
										"SELECT COUNT(*) FROM users;",
									},
									Resources: corev1.ResourceRequirements{
										Requests: corev1.ResourceList{
											corev1.ResourceCPU:    resource.MustParse("100m"),
											corev1.ResourceMemory: resource.MustParse("256Mi"),
										},
										Limits: corev1.ResourceList{
											corev1.ResourceCPU:    resource.MustParse("500m"),
											corev1.ResourceMemory: resource.MustParse("1Gi"),
										},
									},
									SecurityContext: &corev1.SecurityContext{
										ReadOnlyRootFilesystem: &[]bool{true}[0],
										Capabilities: &corev1.Capabilities{
											Drop: []corev1.Capability{"ALL"},
										},
									},
									VolumeMounts: []corev1.VolumeMount{
										{
											Name:      "catalog",
											MountPath: "/catalog",
											ReadOnly:  true,
										},
										{
											Name:      "tmp",
											MountPath: "/tmp",
										},
									},
									Env: []corev1.EnvVar{
										{
											Name:  "DUCKDB_S3_ENDPOINT",
											Value: fmt.Sprintf("http://%s.%s.svc.cluster.local:%d", setup.MinioName, setup.MinioNamespace, setup.MinioPort),
										},
										{
											Name: "DUCKDB_S3_ACCESS_KEY_ID",
											ValueFrom: &corev1.EnvVarSource{
												SecretKeyRef: &corev1.SecretKeySelector{
													LocalObjectReference: corev1.LocalObjectReference{
														Name: "minio-creds",
													},
													Key: "access-key",
												},
											},
										},
										{
											Name: "DUCKDB_S3_SECRET_ACCESS_KEY",
											ValueFrom: &corev1.EnvVarSource{
												SecretKeyRef: &corev1.SecretKeySelector{
													LocalObjectReference: corev1.LocalObjectReference{
														Name: "minio-creds",
													},
													Key: "secret-key",
												},
											},
										},
										{
											Name:  "DUCKDB_MEMORY_LIMIT",
											Value: "768MB",
										},
										{
											Name:  "DUCKDB_THREADS",
											Value: "2",
										},
									},
								},
							},
							Volumes: []corev1.Volume{
								{
									Name: "catalog",
									VolumeSource: corev1.VolumeSource{
										PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
											ClaimName: fmt.Sprintf("%s-catalog", CatalogName),
											ReadOnly:  true,
										},
									},
								},
								{
									Name: "tmp",
									VolumeSource: corev1.VolumeSource{
										EmptyDir: &corev1.EmptyDirVolumeSource{},
									},
								},
							},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, queryJob)).Should(Succeed())

			By("Waiting for the query job to complete")
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{
					Name:      queryJob.Name,
					Namespace: queryJob.Namespace,
				}, queryJob)
				if err != nil {
					return false
				}
				return queryJob.Status.Succeeded > 0
			}, timeout, interval).Should(BeTrue())
		})
	})
})
