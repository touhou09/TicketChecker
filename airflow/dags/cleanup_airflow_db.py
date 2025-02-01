from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pytz

# 한국 시간 설정
KST = pytz.timezone('Asia/Seoul')

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(KST),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ✅ Airflow DB 정리 DAG
with DAG(
    "cleanup_airflow_db",
    default_args=default_args,
    schedule_interval="0 0 * * 0",  # 매주 일요일 실행
    catchup=False,
) as dag:
    cleanup_task = BashOperator(
        task_id="cleanup_airflow_metadata",
        bash_command="airflow db cleanup --keep-last 30 >> /opt/airflow/logs/db_cleanup.log 2>&1",
    )