from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import os

# Fetch Kaggle credentials from Airflow connection
conn = BaseHook.get_connection("kaggle_api")
kaggle_username = conn.login
kaggle_key = conn.password  # API Key

# Set Kaggle credentials as environment variables
os.environ["KAGGLE_USERNAME"] = kaggle_username
os.environ["KAGGLE_KEY"] = kaggle_key

default_args = {"start_date": datetime(2024, 3, 13)}

with DAG("project_dag", default_args=default_args, schedule_interval='@weekly', catchup=False) as dag:
    
    # Download dataset from Kaggle
    download_task = BashOperator(
        task_id="download_dataset_from_kaggle",
        bash_command="kaggle datasets download -d davidcariboo/player-scores -p /tmp/kaggle_download --unzip"
    )
    
    # Upload files to GCS
    upload_task = LocalFilesystemToGCSOperator(
        task_id="upload_dataset_to_gcs",
        src='/tmp/kaggle_download/*',  # Path to downloaded files
        dst='kaggle_dataset/',        # Destination path in GCS
        bucket="<gcs_bucket_name>"
        gcp_conn_id='google_cloud_default',  
    )

    # Dataproc submit job task to process the gcs dataset and overwrite bigquery tables
    submit_pyspark_job_task = DataprocSubmitJobOperator(
        task_id="submit_pyspark_job",
        region="europe-west2", 
        job={
            "reference": {"project_id": "<gcp project id>"},
            "placement": {
                "cluster_name": "<dataproc cluster name>"
            },
            "pyspark_job": {
                "main_python_file_uri": "<path to pyspark script in gcs>",
                "args": [],
                "jar_file_uris": ["gs://spark-lib/bigquery/spark-3.4-bigquery-0.37.0.jar"]
            }
        },
        gcp_conn_id="google_cloud_default",  
    )

    # dbt Cloud task to run the job
    dbt_build_task = DbtCloudRunJobOperator(
        task_id='build_dbt_models',
        job_id=<dbt job id>,
        dbt_cloud_conn_id='dbt_cloud_default',
    )

    download_task >> upload_task >> submit_pyspark_job_task >> dbt_build_task
