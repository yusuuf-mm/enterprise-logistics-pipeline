"""
NaijaMove Logistics Pipeline - Ingestion DAG
Description: Orchestrates daily simulation data batch generation locally
             and transfers it securely to Google Cloud Storage (GCS) landing buckets.
             Refactored using Airflow 3.0 TaskFlow API standards.
"""
import os
import subprocess
from datetime import datetime, timedelta
from typing import Dict, Any
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator

# --- Environmental & Configuration Constants ---
LOCAL_OUTPUT_DIR = "/opt/airflow/data"
GCS_BUCKET = "3mtt-mentees-bucket"
GCS_BLOB_PATH = "yusuf/landing/raw/shipments/year={{ logical_date.strftime('%Y') }}/month={{ logical_date.strftime('%m') }}/shipments_{{ ds }}.ndjson"
GENERATOR_SCRIPT = "/opt/airflow/scripts/generate_shipment_batch.py"

default_args: Dict[str, Any] = {
    "owner": "airflow",
    "start_date": datetime(2026, 7, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="naijamove_ingestion_pipeline",
    default_args=default_args,
    description="NaijaMove Logistics Daily Ingestion Pipeline",
    schedule="@daily",
    catchup=False,
    tags=["logistics", "ingestion", "gcs"],
)
def naijamove_ingestion_pipeline():
    """
    Main DAG workflow defined using the Airflow 3 TaskFlow API.
    """

    @task(task_id="generate_shipments_locally")
    def generate_shipments(logical_date_str: str) -> str:
        """
        Executes the standalone generator script in an isolated subprocess,
        generating batch NDJSON records for the specific logical date.
        """
        output_filename = f"shipments_{logical_date_str}.ndjson"
        target_filepath = os.path.join(LOCAL_OUTPUT_DIR, output_filename)

        print(f"[DAG ENTRY] Starting local shipment dataset compilation for date: {logical_date_str}")
        result = subprocess.run(
            ["python", GENERATOR_SCRIPT, "--output", target_filepath, "--date", logical_date_str],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            print(f"[ERROR ENGINE] Generator process failed! Stderr:\n{result.stderr}")
            raise RuntimeError(f"Data generation subprocess failed: {result.stderr}")

        print(f"[DAG ENTRY] Generation process completed successfully:\n{result.stdout}")
        return target_filepath

    # Task 2: Upload local batch securely using HMAC S3-interoperability
    upload_raw_to_gcs_landing = LocalFilesystemToS3Operator(
        task_id="upload_raw_to_gcs_landing",
        filename=os.path.join(LOCAL_OUTPUT_DIR, "shipments_{{ ds }}.ndjson"),
        dest_key=GCS_BLOB_PATH,
        dest_bucket=GCS_BUCKET,
        aws_conn_id="gcp_hmac_connection",
        replace=True,
        s3_extra_args={"ChecksumAlgorithm": None},  # Explicitly disable AWS native S3 checksum headers for GCS compatibility
    )

    # Define explicit task dependency chain
    generate_shipments(logical_date_str="{{ ds }}") >> upload_raw_to_gcs_landing


# Instantiate the DAG wrapper — MUST be at module level (no indentation)
dag = naijamove_ingestion_pipeline()
