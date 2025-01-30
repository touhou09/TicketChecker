from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime, timedelta
import pytz
import json

# 한국 시간 설정
KST = pytz.timezone('Asia/Seoul')

# GCS 연결 확인 DAG
GCS_BUCKET_NAME = "ticket_checker_bucket"
GCS_OBJECT_NAME = "airflow_check/result.json"

def check_gcs_connection():
    hook = GCSHook()
    try:
        hook.upload(bucket_name=GCS_BUCKET_NAME, object_name=GCS_OBJECT_NAME, data=json.dumps({"result": "success"}))
    except Exception as e:
        hook.upload(bucket_name=GCS_BUCKET_NAME, object_name=GCS_OBJECT_NAME, data=json.dumps({"result": str(e)}))

define_dag = lambda dag_id, schedule_interval, default_args: DAG(
    dag_id,
    default_args=default_args,
    schedule_interval=schedule_interval,
    catchup=False
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(KST),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with define_dag('gcs_connection_check', '@daily', default_args) as dag1:
    gcs_task = PythonOperator(
        task_id='check_gcs',
        python_callable=check_gcs_connection
    )

with DAG(
    "cleanup_airflow_db",
    default_args=default_args,
    schedule_interval="0 0 * * 0",  # 매주 일요일 실행
    catchup=False,
) as dag2:
    cleanup_task = BashOperator(
        task_id="cleanup_airflow_metadata",
        bash_command="airflow db cleanup --keep-last 30 >> /opt/airflow/logs/db_cleanup.log 2>&1",
    )
