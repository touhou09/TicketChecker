import sys
import os
from airflow import DAG
from airflow.dags.naver_flight_dag import fetch_transform_data, upload_to_bigquery
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from datetime import datetime, timedelta
import pytz
import json
import pandas as pd

# 한국 시간 설정
KST = pytz.timezone('Asia/Seoul')

# Airflow Connection 설정
GCP_CONN_ID = "google_cloud_default"
GCS_BUCKET_NAME = "ticket_checker_bucket"
GCS_OBJECT_NAME = "naver_flight_data/flight_results_20250201.json"
BQ_PROJECT_ID = "ticketchecker-449405"
BQ_DATASET_NAME = "ticket_checker"
BQ_TABLE_NAME = "flight_lowest_price"



# DAG 정의
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(KST),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    "gcs_to_bigquery_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

# GCS에서 JSON 데이터를 가져와 변환하는 Task
fetch_transform_task = PythonOperator(
    task_id="fetch_transform_data",
    python_callable=fetch_transform_data,
    provide_context=True,
    dag=dag,
)

# 변환된 데이터를 BigQuery에 업로드하는 Task
upload_task = PythonOperator(
    task_id="upload_to_bigquery",
    python_callable=upload_to_bigquery,
    provide_context=True,
    dag=dag,
)

fetch_transform_task >> upload_task
