from datetime import timedelta, datetime
from airflow import DAG
from airflow import models
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)

from airflow.providers.google.cloud.sensors.dataproc import DataprocJobSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

CLUSTER_NAME = 'dataproc-airflow-cluster'
REGION = 'us-central1'
PROJECT_ID = 'eng-origin-408719'
BUCKET_NAME = 'udacity_song'
PYSPARK_URI = f'gs://udacity_song/spark-job/pyspark_code_etl.py'

PYSPARK_JOB = {
     "reference": {"project_id": PROJECT_ID},
     "placement": {"cluster_name": CLUSTER_NAME},
     "pyspark_job": {"main_python_file_uri": PYSPARK_URI},
}

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n2-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 500},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n2-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 500},
    }
}

# Define default_args dictionary
default_dag_args = {
    'owner': 'Archana',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup': False,
    'tags': ["dataproc_airflow"],
}

with models.DAG(
    "dataproc_airflow_gcp",
    schedule_interval=None,
    default_args=default_dag_args,
    tags=["dataproc_airflow"],
) as dag:



# tasks and operators

    start_pipeline = DummyOperator(
        task_id = 'start_pipeline',
        dag = dag
        )
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )
    pyspark_task = DataprocSubmitJobOperator(
        task_id="pyspark_task", 
        job=PYSPARK_JOB,
        region=REGION,
        project_id=PROJECT_ID,
    )
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster", 
        project_id=PROJECT_ID, 
        cluster_name=CLUSTER_NAME, 
        region=REGION,
    )
    finish_pipeline = DummyOperator(
        task_id = 'finish_pipeline',
        dag = dag
    )

start_pipeline >> create_cluster >> pyspark_task >> delete_cluster >> finish_pipeline
