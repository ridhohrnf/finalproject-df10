import os
import sys
import zipfile
import subprocess

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import pandas as pd

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.models.variable import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from google.cloud import storage
import pyarrow.csv as pv
import pyarrow.parquet as pq


DATASET_ID = Variable.get("DATASET_ID")
BASE_PATH = Variable.get("BASE_PATH")
BUCKET_NAME = Variable.get("BUCKET_NAME")
CLUSTERS = Variable.get("CLUSTERS")
GOOGLE_CLOUD_CONN_ID = Variable.get("GOOGLE_CLOUD_CONN_ID")
BIGQUERY_TABLE_NAME = "online_payment"
GCS_OBJECT_NAME = "online_payment.parquet"
DATA_PATH = f"{BASE_PATH}/datasets"
OUT_PATH = f"{DATA_PATH}/{GCS_OBJECT_NAME}"


spark_master = "spark://spark:7077"

partitioning = {
    "type": "DAY",
    "field": "date",
    "expiration_ms": 2592000000  # Miliseconds dalam 30 hari
}

def download_from_gdrive():
    url = "https://drive.google.com/u/0/uc?id=180XGc2Zy25zcZBAOXQUX4oKraHobt7g-&export=download"
    output = f"{DATA_PATH}/online_payment.zip"
    subprocess.run(["curl", "-L", url, "-o", output])


def unzip_file():
    with zipfile.ZipFile(f"{DATA_PATH}/online_payment.zip", "r") as zip_ref:
        zip_ref.extractall(f"{DATA_PATH}")
    old_name = f"{DATA_PATH}/op.csv"
    new_name = f"{DATA_PATH}/online_payment.csv"
    os.rename(old_name, new_name)


def format_to_parquet(src_file=f"{DATA_PATH}/online_payment.csv"):
    table = pv.read_csv(src_file)
    return pq.write_table(table, src_file.replace(".csv", ".parquet"))

default_args = {
    "owner": "kelompok 4 airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="final_project_df10",
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    tags=["online_payment", "final_project", "kelompok4", "df10"],
) as dag:
    start = DummyOperator(task_id="start")

    download_from_gdrive_task = PythonOperator(
        task_id="download_from_gdrive_task", 
        python_callable=download_from_gdrive
    )

    unzip_file_task = PythonOperator(
        task_id="unzip_file_task", 
        python_callable=unzip_file
    )

    # spark_transform_task = BashOperator(
    #     task_id="spark_transform_task",
    #     bash_command="cd /opt/airflow/spark && python spark_transform.py"
    # )

    spark_transform_task = BashOperator(
        task_id="spark_transform_task",
        bash_command="cd /opt/airflow/spark && python spark_transform.py"
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task", 
        python_callable=format_to_parquet
    )

    stored_data_gcs = LocalFilesystemToGCSOperator(
        task_id="store_to_gcs",
        gcp_conn_id=GOOGLE_CLOUD_CONN_ID,
        src=OUT_PATH,
        dst=GCS_OBJECT_NAME,
        bucket=BUCKET_NAME,
    )

    loaded_data_bigquery = GCSToBigQueryOperator(
        task_id="load_to_bigquery",
        time_partitioning=partitioning,
        cluster_fields=CLUSTERS,
        gcp_conn_id=GOOGLE_CLOUD_CONN_ID,
        bucket=BUCKET_NAME,
        source_objects=[GCS_OBJECT_NAME],
        destination_project_dataset_table=f"{DATASET_ID}.{BIGQUERY_TABLE_NAME}",
        schema_fields=[  # based on https://cloud.google.com/bigquery/docs/schemas
            {"name": "step", "type": "INT64", "mode": "NULLABLE"},
            {"name": "type", "type": "STRING", "mode": "NULLABLE"},
            {"name": "amount", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "nameOrig", "type": "STRING", "mode": "NULLABLE"},
            {"name": "oldbalanceOrg", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "newbalanceOrig", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "nameDest", "type": "STRING", "mode": "NULLABLE"},
            {"name": "oldbalanceDest", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "newbalanceDest", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "isFraud", "type": "STRING", "mode": "NULLABLE"},
            {"name": "isFlaggedFraud", "type": "STRING", "mode": "NULLABLE"},
            {"name": "timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "date", "type": "DATE", "mode": "NULLABLE"},
            {"name": "origin", "type": "STRING", "mode": "NULLABLE"},
            {"name": "destination", "type": "STRING", "mode": "NULLABLE"}
        ],
        autodetect=False,
        write_disposition="WRITE_TRUNCATE",
        source_format="PARQUET"  # If the table already exists - overwrites the table data
    )
    end = DummyOperator(task_id="end")

    
    start >> download_from_gdrive_task >> \
    unzip_file_task >> spark_transform_task >> \
    format_to_parquet_task >> stored_data_gcs >> \
    loaded_data_bigquery >> end
