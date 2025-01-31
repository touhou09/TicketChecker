import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime, timedelta
import pytz
import json

sys.path.append(os.path.join(os.environ["AIRFLOW_HOME"], "dags/crawler"))
from crawler.naver_flight_tester import NaverFlightCrawler

# 한국 시간 설정
KST = pytz.timezone('Asia/Seoul')

# GCS 설정
GCS_BUCKET_NAME = "ticket_checker_bucket"
GCS_FILE_PATH = "naver_flight_data/flight_results.json"

# 항공편 데이터를 가져와 JSON으로 저장하는 함수
def fetch_flight_data_and_upload():
    departure_airport = "ICN"  # 인천
    arrival_airport = "KIX"    # 오사카 간사이

    crawler = NaverFlightCrawler()
    
    # 오늘 날짜부터 5개월(150일) 동안의 데이터 수집
    start_date = datetime.now(KST) + timedelta(days=1)
    end_date = start_date + timedelta(days=150)

    flight_data_list = []

    current_date = start_date
    while current_date <= end_date:
        departure_date = current_date.strftime("%Y%m%d")
        print(f"[INFO] {departure_date} 데이터 수집 중...")

        flight_data = crawler.fetch_flight_data(departure_airport, arrival_airport, departure_date)
        if flight_data:
            flight_data_list.append({
                "date": departure_date,
                "data": flight_data
            })

        current_date += timedelta(days=1)

    if flight_data_list:
        today = start_date.strftime("%Y%m%d")
        gcs_path = GCS_FILE_PATH.format(date=today)

        # JSON 데이터를 GCS에 업로드
        hook = GCSHook()
        hook.upload(bucket_name=GCS_BUCKET_NAME, object_name=gcs_path, data=json.dumps(flight_data_list, ensure_ascii=False, indent=4))
        print(f"[INFO] {gcs_path} 경로로 업로드 완료.")
    else:
        print("[INFO] 수집된 데이터가 없습니다.")

# GCS 연결 확인 함수
def check_gcs_connection():
    hook = GCSHook()
    try:
        hook.upload(bucket_name=GCS_BUCKET_NAME, object_name="airflow_check/result.json", data=json.dumps({"result": "success"}))
    except Exception as e:
        hook.upload(bucket_name=GCS_BUCKET_NAME, object_name="airflow_check/result.json", data=json.dumps({"result": str(e)}))

# DAG 설정 함수
def define_dag(dag_id, schedule_interval, default_args):
    return DAG(
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

# DAG1: GCS 연결 확인 DAG
with define_dag('gcs_connection_check', '@daily', default_args) as dag1:
    gcs_task = PythonOperator(
        task_id='check_gcs',
        python_callable=check_gcs_connection
    )

# DAG2: 5개월 간의 항공편 데이터 크롤링 및 GCS 업로드
with define_dag('naver_flight_crawl_gcs', '@daily', default_args) as dag2:
    crawl_and_upload_task = PythonOperator(
        task_id="fetch_and_upload_flight_data",
        python_callable=fetch_flight_data_and_upload
    )
