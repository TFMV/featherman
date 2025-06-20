package query

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
)

func TestValidateSQL(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		ok   bool
	}{
		{"valid select", "SELECT * FROM table", true},
		{"valid insert", "INSERT INTO table VALUES (1, 'test')", true},
		{"valid update", "UPDATE table SET col = 'value'", true},
		{"valid delete", "DELETE FROM table WHERE id = 1", true},
		{"valid create", "CREATE TABLE test (id INT)", true},
		{"valid drop", "DROP TABLE test", true},
		{"lowercase select", "select * from table", true},
		{"mixed case", "Select * From table", true},
		{"banned attach", "ATTACH DATABASE 'path'", false},
		{"banned install", "INSTALL EXTENSION test", false},
		{"banned copy", "COPY table TO 'file.csv'", false},
		{"banned export", "EXPORT DATABASE", false},
		{"attach in lowercase", "attach database 'path'", false},
		{"install in lowercase", "install extension test", false},
		{"copy in lowercase", "copy table to 'file.csv'", false},
		{"export in lowercase", "export database", false},
		{"attach in mixed case", "Attach Database 'path'", false},
		{"complex query with banned word", "SELECT * FROM table WHERE col = 'ATTACH'", true}, // This should be allowed as ATTACH is in a string literal
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := validateSQL(c.sql)
			if c.ok && err != nil {
				t.Fatalf("expected ok for %s: %v", c.sql, err)
			}
			if !c.ok && err == nil {
				t.Fatalf("expected error for %s", c.sql)
			}
		})
	}
}

func TestNewExecutor(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	cfg := &rest.Config{}

	executor := NewExecutor(clientset, cfg)

	if executor == nil {
		t.Fatal("expected executor to be created")
	}

	if executor.client != clientset {
		t.Error("expected client to be set")
	}

	if executor.cfg != cfg {
		t.Error("expected config to be set")
	}
}

func TestExecutorInterface(t *testing.T) {
	// Test that Executor implements ExecutorInterface
	clientset := fake.NewSimpleClientset()
	cfg := &rest.Config{}

	var executor ExecutorInterface = NewExecutor(clientset, cfg)

	if executor == nil {
		t.Fatal("expected executor to implement ExecutorInterface")
	}
}

func TestSelectWarmPod(t *testing.T) {
	// Create a fake pod that should be returned
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "warm-pod-1",
			Namespace: "default",
			Labels: map[string]string{
				"ducklake.featherman.dev/catalog": "test-catalog",
				"warm-pod":                        "true",
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}

	clientset := fake.NewSimpleClientset(pod)
	cfg := &rest.Config{}
	executor := NewExecutor(clientset, cfg)

	ctx := context.Background()
	result, err := executor.selectWarmPod(ctx, "test-catalog")

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if result == nil {
		t.Fatal("expected pod to be returned")
	}

	if result.Name != "warm-pod-1" {
		t.Errorf("expected pod name 'warm-pod-1', got '%s'", result.Name)
	}
}

func TestSelectWarmPodNotFound(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	cfg := &rest.Config{}
	executor := NewExecutor(clientset, cfg)

	ctx := context.Background()
	result, err := executor.selectWarmPod(ctx, "nonexistent-catalog")

	if err == nil {
		t.Fatal("expected error for nonexistent catalog")
	}

	if result != nil {
		t.Error("expected nil pod for nonexistent catalog")
	}

	if err.Error() != "no warm pod" {
		t.Errorf("expected 'no warm pod' error, got '%s'", err.Error())
	}
}

func TestSelectWarmPodWrongLabels(t *testing.T) {
	// Create a pod with wrong labels
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "wrong-pod",
			Namespace: "default",
			Labels: map[string]string{
				"ducklake.featherman.dev/catalog": "different-catalog",
				"warm-pod":                        "true",
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}

	clientset := fake.NewSimpleClientset(pod)
	cfg := &rest.Config{}
	executor := NewExecutor(clientset, cfg)

	ctx := context.Background()
	result, err := executor.selectWarmPod(ctx, "test-catalog")

	if err == nil {
		t.Fatal("expected error for wrong catalog")
	}

	if result != nil {
		t.Error("expected nil pod for wrong catalog")
	}
}
