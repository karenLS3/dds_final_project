# Dataproc Flask API

A Flask REST API service for managing Google Cloud Dataproc Spark jobs and retrieving results.

## Installation

```bash
pip install -r requirements.txt
```

## Running the Service

```bash
python flask_app.py
```

The service will start on `http://localhost:5001`

## Configuration (Environment Variables)

This service is configured via environment variables:

- PORT (default: 5001)
- PROJECT_ID
- REGION
- CLUSTER_NAME
- BUCKET_NAME

For local development, create a `.env` file (not committed).

## API Endpoints

### 1. Create Spark Job
**POST** `/create/job`

Submit a new PySpark job to Dataproc cluster.

**Request Body:**
```json
{
  "main_python_file": "gs://YOUR_BUCKET/path/to/job.py",
  "args": ["arg1", "arg2"],
  "cluster_name": "YOUR_CLUSTER_NAME"
}
```

**Response:**
```json
{
  "status": "success",
  "message": "Job submitted successfully",
  "job_id": "abc123xyz",
  "cluster": "YOUR_CLUSTER_NAME",
  "main_file": "gs://YOUR_BUCKET/path/to/job.py"
}
```

**Example:**
```bash
curl -X POST http://localhost:5001/create/job \
  -H "Content-Type: application/json" \
  -d '{
    "main_python_file": "gs://YOUR_BUCKET/path/to/job.py",
    "args": ["arg1", "arg2"]
  }'
```

---

### 2. Get Job Status
**GET** `/spark/job/status?job_id=<JOB_ID>`

Retrieve the status and details of a running or completed Spark job.

**Query Parameters:**
- `job_id` (required): The ID of the job to check

**Response:**
```json
{
  "job_id": "abc123xyz",
  "status": "DONE",
  "cluster": "YOUR_CLUSTER_NAME",
  "details": null,
  "state_start_time": "2025-12-06 10:30:00",
  "main_python_file": "gs://YOUR_BUCKET/path/to/job.py",
  "args": ["arg1", "arg2"],
  "driver_output_uri": "gs://bucket/output/path",
  "completed": true
}
```

**Example:**
```bash
curl "http://localhost:5001/spark/job/status?job_id=abc123xyz"
```

---

### 3. Get Results from Bucket
**GET** `/results?path=<PATH_PREFIX>&bucket=<BUCKET_NAME>`

Read `problem_1.json` and `problem_2.json` from Google Cloud Storage.

**Query Parameters:**
- `bucket` (optional): Bucket name (default: `YOUR_BUCKET/`)
- `path` (optional): Path prefix to the JSON files (default: root)

**Response:**
```json
{
  "status": "success",
  "bucket": "YOUR_BUCKET",
  "results": {
    "problem_1": {
      "data": "contents of problem_1.json"
    },
    "problem_2": {
      "data": "contents of problem_2.json"
    }
  }
}
```

**Examples:**
```bash
# Read from root of default bucket
curl "http://localhost:5001/results"

# Read from specific path
curl "http://localhost:5001/results?path=BUCKET/results"

# Read from different bucket and path
curl "http://localhost:5001/results?bucket=my-bucket&path=output"
```

---

### 4. Health Check
**GET** `/health`

Check if the service is running.

**Response:**
```json
{
  "status": "healthy",
  "service": "Dataproc Flask API",
  "project_id": "YOUR_PROJECT_ID",
  "region": "YOUR_REGION"
}
```

**Example:**
```bash
curl "http://localhost:5001/health"
```

## Complete Workflow Example

### 1. Submit a Job
```bash
curl -X POST http://localhost:5001/create/job \
  -H "Content-Type: application/json" \
  -d '{
    "main_python_file": "gs://YOUR_BUCKET/path/to/job.py",
    "args": ["input_data", "output_path"]
  }'
```

Response:
```json
{
  "job_id": "job-12345",
  "status": "success"
}
```

### 2. Check Job Status
```bash
curl "http://localhost:5001/spark/job/status?job_id=job-12345"
```

### 3. Get Results (once job completes)
```bash
curl "http://localhost:5001/results?path=output"
```

## Configuration

Update the following constants in `flask_app.py` to match your environment:

```python
PROJECT_ID = "YOUR_PROJECT_ID"
REGION = "YOUR_REGION"
CLUSTER_NAME = "YOUR_CLUSTER_NAME"
BUCKET_NAME = "YOUR_BUCKET"
GCS_BUCKET = "YOUR_BUCKET"
GCS_BASE_PATH = "YOUR_PROJECT"
```

## Authentication

Ensure Google Cloud credentials are configured:

```bash
gcloud auth application-default login
```

## Error Handling

All endpoints return appropriate HTTP status codes:
- `200`: Success
- `201`: Created (job submission)
- `400`: Bad Request (missing parameters)
- `500`: Internal Server Error

Error responses include:
```json
{
  "status": "error",
  "message": "Error description"
}
```
