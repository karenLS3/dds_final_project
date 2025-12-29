# Docker & Kubernetes Deployment Guide

This guide explains how to containerize and deploy the Dataproc Flask API and configure Google Cloud authentication.

## Prerequisites
- Docker installed
- Access to a container registry (e.g., GCR, GAR, Docker Hub)
- Google Cloud project access
- kubectl configured for your cluster (for Kubernetes deployment)

## 1) Build & Run Locally with Docker

### Run
### Authentication Options (Local)
- Option A: Use local ADC (recommended for local testing)
  - Make sure you've run: `gcloud auth application-default login`
  - Then run container with `-v ~/.config/gcloud:/root/.config/gcloud:ro` to mount credentials inside container:
  ```bash
  docker run --rm -p 5001:5001 \
    -v $HOME/.config/gcloud:/root/.config/gcloud:ro \
    -e PORT=5001 \
    -e PROJECT_ID=YOUR_PROJECT_ID \
    -e REGION=YOUR_REGION \
    -e CLUSTER_NAME=YOUR_CLUSTER \
    -e BUCKET_NAME=YOUR_BUCKET \
      YOUR_REGISTRY/dataproc-flask-api:v1
  ```


## 2) Push Image to Registry
```bash
# Example: Google Artifact Registry
PROJECT_ID=YOUR_PROJECT_ID
REGION=YOUR_REGION
REPO=dataproc-api
IMAGE=dataproc-flask-api
TAG=v1

# Configure auth (once)
gcloud auth configure-docker ${REGION}-docker.pkg.dev

# Build & push
docker build -t ${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/${IMAGE}:${TAG} .
docker push ${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/${IMAGE}:${TAG}
```

## 3) Kubernetes Deployment

Update `k8s/deployment.yaml` to use your pushed image:
```yaml
containers:
  - name: api
    image: your-region-docker.pkg.dev/YOUR_PROJECT_ID/dataproc-api/dataproc-flask-api:v1
```

### Authentication on GKE
- Recommended: **Workload Identity**
  1. Create a GCP service account with required roles (Dataproc Admin, Storage Object Viewer as needed).
  2. Create a Kubernetes service account and annotate it to impersonate the GCP service account.
  3. Set `serviceAccountName` in the Deployment to use that K8s service account.

Resources:
- https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity

- Alternative (less secure): Mount a secret with SA key file and set `GOOGLE_APPLICATION_CREDENTIALS`. See commented lines in `deployment.yaml`.

### Apply Manifests
```bash
kubectl apply -f k8s/deployment.yaml
kubectl apply -f k8s/service.yaml
```

### Access the Service
- If using ClusterIP, port-forward:
```bash
kubectl port-forward deployment/dataproc-flask-api 5001:5001
```
Visit `http://localhost:5001/health`

- Or change Service type to LoadBalancer for external access.

## 4) Environment Variables
The app reads the following env vars:
- `PORT` (default 5001)
- `PROJECT_ID`
- `REGION`
- `CLUSTER_NAME`
- `BUCKET_NAME`

You can override these in Docker run or Kubernetes manifests.

## 5) Health Checks
- Container has a healthcheck hitting `/health`
- Kubernetes liveness/readiness probes are configured in `deployment.yaml`

## 6) Troubleshooting
- 401/403: Verify credentials and IAM roles for Dataproc and GCS.
- Timeout: Ensure cluster and region values are correct; network egress allowed.
- Logs: Check container logs: `kubectl logs -l app=dataproc-flask-api`