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

# Define variables
CLUSTER_NAME="featherman-e2e"
REGISTRY_NAME="kind-registry"
REGISTRY_PORT="5001"

# Create registry container unless it already exists
if ! docker ps --filter name=^/${REGISTRY_NAME}$ --format '{{.Names}}' | grep -q "^${REGISTRY_NAME}$"; then
    echo "Creating local registry container..."
    docker run -d --restart=always -p "${REGISTRY_PORT}:5000" --name "${REGISTRY_NAME}" registry:2
fi

# Delete existing cluster if it exists
if kind get clusters | grep -q "^${CLUSTER_NAME}$"; then
    echo "Deleting existing KinD cluster..."
    kind delete cluster --name "${CLUSTER_NAME}"
fi

# Create KinD cluster config with registry config
cat <<EOF > kind-config.yaml
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
containerdConfigPatches:
- |-
  [plugins."io.containerd.grpc.v1.cri".registry.mirrors."localhost:${REGISTRY_PORT}"]
    endpoint = ["http://${REGISTRY_NAME}:5000"]
nodes:
- role: control-plane
  extraPortMappings:
  - containerPort: 9000  # For MinIO
    hostPort: 9000
  - containerPort: 9001  # For MinIO Console
    hostPort: 9001
EOF

# Create cluster
echo "Creating KinD cluster..."
kind create cluster --name "${CLUSTER_NAME}" --config kind-config.yaml

# Connect the registry to the cluster network if not already connected
if ! docker network inspect kind | grep -q "${REGISTRY_NAME}"; then
    echo "Connecting registry to cluster network..."
    docker network connect "kind" "${REGISTRY_NAME}"
fi

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
        - name: MINIO_ACCESS_KEY
          valueFrom:
            secretKeyRef:
              name: minio-creds
              key: access-key
        - name: MINIO_SECRET_KEY
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
CONTROLLER_IMG="localhost:${REGISTRY_PORT}/featherman-controller:latest"
make docker-build IMG="${CONTROLLER_IMG}"
docker push "${CONTROLLER_IMG}"

# Deploy the controller
echo "Deploying controller..."
make deploy IMG="${CONTROLLER_IMG}"

# Wait for controller to be ready
echo "Waiting for controller to be ready..."
kubectl -n featherman-system wait --for=condition=Available deployment/featherman-controller-manager --timeout=60s

echo "KinD cluster setup complete!"
echo "Cluster name: ${CLUSTER_NAME}"
echo "MinIO API endpoint: minio.minio-test.svc.cluster.local:9000"
echo "MinIO Console: http://localhost:9001"
echo "MinIO credentials: minioadmin / minioadmin"
echo "To delete the cluster: kind delete cluster --name ${CLUSTER_NAME}" 