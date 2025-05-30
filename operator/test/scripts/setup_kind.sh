#!/bin/bash
set -e

# Check if KinD is installed
if ! command -v kind &> /dev/null; then
    echo "KinD is not installed. Installing..."
    brew install kind
fi

# Check if kubectl is installed
if ! command -v kubectl &> /dev/null; then
    echo "kubectl is not installed. Installing..."
    brew install kubectl
fi

# Define cluster name
CLUSTER_NAME="featherman-e2e"
CONTROLLER_IMG="featherman-controller:latest"

# Delete existing cluster if it exists
if kind get clusters | grep -q "^${CLUSTER_NAME}$"; then
    echo "Deleting existing KinD cluster..."
    kind delete cluster --name "${CLUSTER_NAME}"
fi

# Create KinD cluster config
cat <<EOF > kind-config.yaml
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
  extraPortMappings:
  - containerPort: 9000  # For MinIO API
    hostPort: 9200
  - containerPort: 9001  # For MinIO Console
    hostPort: 9201
  extraMounts:
  - hostPath: /tmp/featherman-e2e
    containerPath: /var/lib/featherman
  kubeadmConfigPatches:
  - |
    kind: InitConfiguration
    nodeRegistration:
      kubeletExtraArgs:
        node-labels: "ingress-ready=true"
EOF

# Create cluster
echo "Creating KinD cluster..."
kind create cluster --name "${CLUSTER_NAME}" --config kind-config.yaml

# Wait for cluster to be ready
echo "Waiting for cluster to be ready..."
kubectl wait --for=condition=Ready nodes --all --timeout=60s

# Create storage class
kubectl apply -f - <<EOF
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: standard
provisioner: rancher.io/local-path
volumeBindingMode: WaitForFirstConsumer
EOF

# Create MinIO namespace
echo "Creating MinIO namespace..."
kubectl create namespace minio-test

# Deploy MinIO
echo "Deploying MinIO..."
kubectl -n minio-test apply -f - <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: minio-creds
type: Opaque
stringData:
  access-key: minioadmin
  secret-key: minioadmin
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: minio
spec:
  selector:
    matchLabels:
      app: minio
  strategy:
    type: Recreate
  template:
    metadata:
      labels:
        app: minio
    spec:
      containers:
      - name: minio
        image: minio/minio:latest
        args:
        - server
        - /data
        - --console-address
        - ":9001"
        env:
        - name: MINIO_ROOT_USER
          valueFrom:
            secretKeyRef:
              name: minio-creds
              key: access-key
        - name: MINIO_ROOT_PASSWORD
          valueFrom:
            secretKeyRef:
              name: minio-creds
              key: secret-key
        ports:
        - containerPort: 9000
        - containerPort: 9001
        volumeMounts:
        - name: data
          mountPath: /data
        resources:
          requests:
            memory: "512Mi"
            cpu: "100m"
          limits:
            memory: "1Gi"
            cpu: "200m"
      volumes:
      - name: data
        emptyDir: {}
---
apiVersion: v1
kind: Service
metadata:
  name: minio
spec:
  ports:
  - name: api
    port: 9000
    targetPort: 9000
  - name: console
    port: 9001
    targetPort: 9001
  selector:
    app: minio
EOF

# Wait for MinIO to be ready
echo "Waiting for MinIO to be ready..."
kubectl -n minio-test wait --for=condition=Available deployment/minio --timeout=60s

# Install CRDs
echo "Installing CRDs..."
make install

# Build and load the controller image
echo "Building and loading controller image..."
make docker-build IMG="${CONTROLLER_IMG}"
kind load docker-image "${CONTROLLER_IMG}" --name "${CLUSTER_NAME}"

# Deploy the controller
echo "Deploying controller..."
make deploy IMG="${CONTROLLER_IMG}"

# Wait for controller to be ready
echo "Waiting for controller to be ready..."
kubectl -n featherman-system wait --for=condition=Available deployment/featherman-controller-manager --timeout=60s

echo "KinD cluster setup complete!"
echo "Cluster name: ${CLUSTER_NAME}"
echo "MinIO API endpoint: minio.minio-test.svc.cluster.local:9000"
echo "MinIO Console: http://localhost:9201"
echo "MinIO credentials: minioadmin / minioadmin"
echo "To delete the cluster: kind delete cluster --name ${CLUSTER_NAME}" 