package setup

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	MinioNamespace = "minio-test"
	MinioName      = "minio"
	MinioPort      = 9000
	MinioAccessKey = "minioadmin"
	MinioSecretKey = "minioadmin"
)

// TestEnv manages the test environment
type TestEnv struct {
	Client client.Client
}

// NewTestEnv creates a new test environment
func NewTestEnv(client client.Client) *TestEnv {
	return &TestEnv{
		Client: client,
	}
}

// Setup prepares the test environment
func (e *TestEnv) Setup(ctx context.Context) error {
	// Check if MinIO namespace exists
	ns := &corev1.Namespace{}
	err := e.Client.Get(ctx, client.ObjectKey{Name: MinioNamespace}, ns)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to check namespace: %w", err)
		}
		// Namespace doesn't exist, create it and set up MinIO
		if err := e.setupMinioNamespace(ctx); err != nil {
			return fmt.Errorf("failed to setup namespace: %w", err)
		}

		if err := e.setupMinioSecret(ctx); err != nil {
			return fmt.Errorf("failed to setup secret: %w", err)
		}

		if err := e.setupMinioDeployment(ctx); err != nil {
			return fmt.Errorf("failed to setup deployment: %w", err)
		}

		if err := e.setupMinioService(ctx); err != nil {
			return fmt.Errorf("failed to setup service: %w", err)
		}
	}

	// Always wait for MinIO to be ready
	return e.waitForMinio(ctx)
}

// Teardown cleans up the test environment
func (e *TestEnv) Teardown(ctx context.Context) error {
	// Delete namespace and everything in it
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: MinioNamespace,
		},
	}
	return e.Client.Delete(ctx, ns)
}

func (e *TestEnv) setupMinioNamespace(ctx context.Context) error {
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: MinioNamespace,
		},
	}
	if err := e.Client.Create(ctx, ns); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return err
		}
	}
	return nil
}

func (e *TestEnv) setupMinioSecret(ctx context.Context) error {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "minio-creds",
			Namespace: MinioNamespace,
		},
		Type: corev1.SecretTypeOpaque,
		StringData: map[string]string{
			"access-key": MinioAccessKey,
			"secret-key": MinioSecretKey,
		},
	}
	return e.Client.Create(ctx, secret)
}

func (e *TestEnv) setupMinioDeployment(ctx context.Context) error {
	replicas := int32(1)
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      MinioName,
			Namespace: MinioNamespace,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": MinioName,
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app": MinioName,
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  MinioName,
							Image: "minio/minio:latest",
							Args: []string{
								"server",
								"/data",
								"--console-address",
								":9001",
							},
							Env: []corev1.EnvVar{
								{
									Name: "MINIO_ACCESS_KEY",
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
									Name: "MINIO_SECRET_KEY",
									ValueFrom: &corev1.EnvVarSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: "minio-creds",
											},
											Key: "secret-key",
										},
									},
								},
							},
							Ports: []corev1.ContainerPort{
								{
									ContainerPort: MinioPort,
								},
								{
									ContainerPort: 9001,
								},
							},
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("100m"),
									corev1.ResourceMemory: resource.MustParse("512Mi"),
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("200m"),
									corev1.ResourceMemory: resource.MustParse("1Gi"),
								},
							},
						},
					},
				},
			},
		},
	}
	return e.Client.Create(ctx, deployment)
}

func (e *TestEnv) setupMinioService(ctx context.Context) error {
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      MinioName,
			Namespace: MinioNamespace,
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{
					Name:       "api",
					Port:       MinioPort,
					TargetPort: intstr.FromInt(MinioPort),
				},
				{
					Name:       "console",
					Port:       9001,
					TargetPort: intstr.FromInt(9001),
				},
			},
			Selector: map[string]string{
				"app": MinioName,
			},
		},
	}
	return e.Client.Create(ctx, service)
}

func (e *TestEnv) waitForMinio(ctx context.Context) error {
	// Wait for deployment to be ready
	deployment := &appsv1.Deployment{}
	key := client.ObjectKey{
		Namespace: MinioNamespace,
		Name:      MinioName,
	}

	// Poll every 2 seconds for 60 seconds
	for i := 0; i < 30; i++ {
		if err := e.Client.Get(ctx, key, deployment); err != nil {
			time.Sleep(2 * time.Second)
			continue
		}

		if deployment.Status.ReadyReplicas == *deployment.Spec.Replicas {
			return nil
		}

		time.Sleep(2 * time.Second)
	}

	return fmt.Errorf("timeout waiting for MinIO to be ready")
}
