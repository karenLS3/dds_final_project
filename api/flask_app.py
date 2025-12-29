from flask import Flask, request, jsonify
from google.cloud import dataproc_v1 as dataproc
from google.cloud import storage
from google.cloud.dataproc_v1.types import Job, JobPlacement, PySparkJob
import json
import os
from dotenv import load_dotenv
load_dotenv()

app = Flask(__name__)

# -------------------------------
# CONFIGURATION
# -------------------------------

PROJECT_ID = os.getenv("PROJECT_ID", "your-project-id")
REGION = os.getenv("REGION", "us-central1")
CLUSTER_NAME = os.getenv("CLUSTER_NAME", "cluster-dataproc")
BUCKET_NAME = os.getenv("BUCKET_NAME", "storage-dataproc-cluster-bucket")
PORT = int(os.getenv("PORT", 5001))

# Initialize clients
job_client = dataproc.JobControllerClient(
    client_options={"api_endpoint": f"{REGION}-dataproc.googleapis.com:443"}
)
storage_client = storage.Client(project=PROJECT_ID)

# -------------------------------
# ENDPOINT 1: CREATE SPARK JOB
# -------------------------------

@app.route('/create/job', methods=['POST'])
def create_job():
    """
    Create and submit a PySpark job to Dataproc.
    
    Expected JSON body:
    {
        "main_python_file": "gs://bucket/path/to/script.py",
        "args": ["arg1", "arg2"],  # optional
        "cluster_name": "cluster-name"  # optional, uses default if not provided
    }
    """
    try:
        data = request.get_json()
        
        if not data or 'main_python_file' not in data:
            return jsonify({
                "error": "Missing required field: main_python_file"
            }), 400
        
        main_python_file = data['main_python_file']
        args = data.get('args', [])
        cluster_name = data.get('cluster_name', CLUSTER_NAME)
        
        # Create PySpark job
        pyspark_job = PySparkJob(
            main_python_file_uri=main_python_file,
            args=args
        )
        
        job = Job(
            placement=JobPlacement(cluster_name=cluster_name),
            pyspark_job=pyspark_job
        )
        
        # Submit job
        result = job_client.submit_job(
            request={"project_id": PROJECT_ID, "region": REGION, "job": job}
        )
        
        job_id = result.reference.job_id
        
        return jsonify({
            "status": "success",
            "message": "Job submitted successfully",
            "job_id": job_id,
            "cluster": cluster_name,
            "main_file": main_python_file
        }), 201
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

# -------------------------------
# ENDPOINT 2: GET JOB STATUS
# -------------------------------

@app.route('/spark/job/status', methods=['GET'])
def get_job_status():
    """
    Get the status of a Spark job.
    
    Query parameters:
    - job_id: The ID of the job to check (required)
    """
    try:
        job_id = request.args.get('job_id')
        
        if not job_id:
            return jsonify({
                "error": "Missing required parameter: job_id"
            }), 400
        
        # Get job details
        job = job_client.get_job(
            project_id=PROJECT_ID,
            region=REGION,
            job_id=job_id
        )
        
        # Prepare response
        response = {
            "job_id": job.reference.job_id,
            "status": job.status.state.name,
            "cluster": job.placement.cluster_name,
            "details": job.status.details if job.status.details else None,
            "state_start_time": str(job.status.state_start_time) if job.status.state_start_time else None
        }
        
        # Add PySpark job details
        if job.pyspark_job:
            response["main_python_file"] = job.pyspark_job.main_python_file_uri
            if job.pyspark_job.args:
                response["args"] = list(job.pyspark_job.args)
        
        # Add driver output URI if available
        if job.driver_output_resource_uri:
            response["driver_output_uri"] = job.driver_output_resource_uri
        
        # Check if job is completed
        if job.status.state == dataproc.JobStatus.State.DONE:
            response["completed"] = True
        elif job.status.state == dataproc.JobStatus.State.ERROR:
            response["completed"] = True
            response["failed"] = True
        else:
            response["completed"] = False
        
        return jsonify(response), 200
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

# -------------------------------
# ENDPOINT 3: GET JSON FILES FROM BUCKET
# -------------------------------

@app.route('/results', methods=['GET'])
def get_results():
    """
    Read problem_1.json and problem_2.json from Google Cloud Storage bucket.
    
    Query parameters:
    - bucket: Bucket name (optional, uses default if not provided)
    - path: Path prefix to the JSON files (optional, defaults to root)
    """
    try:
        bucket_name = request.args.get('bucket', BUCKET_NAME)
        path_prefix = request.args.get('path', '')
        
        # Construct file paths
        if path_prefix and not path_prefix.endswith('/'):
            path_prefix += '/'
        
        problem_1_path = f"{path_prefix}problem1/part-00000"
        problem_2_path = f"{path_prefix}problem2/part-00000"
        
        bucket = storage_client.bucket(bucket_name)
        
        results = {}
        client_error = False
        # Read problem_1.json
        try:
            blob_1 = bucket.blob(problem_1_path)
            if blob_1.exists():
                content_1 = blob_1.download_as_text()
                results["problem_1"] = json.loads(content_1)
            else:
                results["problem_1"] = {
                    "error": "File not found",
                    "path": f"gs://{bucket_name}/{problem_1_path}"
                }
                client_error = True
        except Exception as e:
            results["problem_1"] = {
                "error": str(e),
                "path": f"gs://{bucket_name}/{problem_1_path}"
            }
        
        # Read problem_2.json
        try:
            blob_2 = bucket.blob(problem_2_path)
            if blob_2.exists():
                content_2 = blob_2.download_as_text()
                results["problem_2"] = json.loads(content_2)
            else:
                results["problem_2"] = {
                    "error": "File not found",
                    "path": f"gs://{bucket_name}/{problem_2_path}"
                }
                client_error = True
        except Exception as e:
            results["problem_2"] = {
                "error": str(e),
                "path": f"gs://{bucket_name}/{problem_2_path}"
            }
            
        
        return jsonify({
            "status": "success" if not client_error else "client_error",
            "bucket": bucket_name,
            "results": results
        }), 200 if not client_error else 400
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500
    

# -------------------------------
# HEALTH CHECK ENDPOINT
# -------------------------------

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "service": "Dataproc Flask API",
        "project_id": PROJECT_ID,
        "region": REGION
    }), 200

# -------------------------------
# RUN APPLICATION
# -------------------------------

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=PORT)
