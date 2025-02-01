import sys
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

from datetime import datetime, timedelta

import pytz
import json
import logging

sys.path.append(os.path.join(os.environ["AIRFLOW_HOME"], "dags/crawler"))
from crawler.naver_flight_tester import NaverFlightCrawler

# 한국 시간 설정
KST = pytz.timezone('Asia/Seoul')

# GCS 및 BigQuery 설정
GCS_BUCKET_NAME = "ticket_checker_bucket"
GCS_OBJECT_NAME = "naver_flight_data/flight_results.json"

GCP_CONN_ID = "google_cloud_default"
BQ_PROJECT_ID = "ticketchecker-449405"
BQ_DATASET_NAME = "ticket_checker"
BQ_TABLE_NAME = "flight_lowest_price"

# GCS 연결 확인 함수
def check_gcs_connection():
    hook = GCSHook()
    try:
        hook.upload(bucket_name=GCS_BUCKET_NAME, object_name="airflow_check/result.json", data=json.dumps({"result": "success"}))
    except Exception as e:
        hook.upload(bucket_name=GCS_BUCKET_NAME, object_name="airflow_check/result.json", data=json.dumps({"result": str(e)}))

# 항공편 데이터를 가져와 JSON으로 저장하는 함수
def fetch_flight_data_and_upload():
    departure_airport = "ICN"  # 인천
    arrival_airport = "KIX"    # 오사카 간사이

    crawler = NaverFlightCrawler()
    
    # 오늘 날짜부터 5개월(150일) 동안의 데이터 수집
    start_date = datetime.now(KST) + timedelta(days=10)
    end_date = start_date + timedelta(days=10)

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
        gcs_path = GCS_OBJECT_NAME.format(date=today)
        
        # JSON 데이터를 GCS에 업로드
        hook = GCSHook()
        hook.upload(bucket_name=GCS_BUCKET_NAME, object_name=gcs_path, data=json.dumps(flight_data_list, ensure_ascii=False, indent=4))
        print(f"[INFO] {gcs_path} 경로로 업로드 완료.")
    else:
        print("[INFO] 수집된 데이터가 없습니다.")

def fetch_transform_data():
    gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
    
    # ✅ GCS에서 JSON 데이터 다운로드
    raw_data = gcs_hook.download(GCS_BUCKET_NAME, GCS_OBJECT_NAME)
    data = json.loads(raw_data)  # JSON 파싱

    transformed_data = []

    for entry in data:  
        date = entry.get("date")  
        fares = entry.get("data", {}).get("fares", {})

        # ✅ 해당 날짜의 최소 요금을 찾기 위한 초기값 설정
        min_fare = float('inf')

        for flight_id, fare_info in fares.items():
            # ✅ A01 요금 타입이 존재하는지 확인
            fare_types = fare_info.get("fare", {}).get("A01", [])

            for fare_option in fare_types:
                adult_fare = fare_option.get("Adult", {}).get("Fare")
                if adult_fare is not None:
                    try:
                        adult_fare = int(adult_fare)  # ✅ 문자열인 경우 정수 변환
                        if adult_fare < min_fare:
                            min_fare = adult_fare
                    except ValueError:
                        logging.warning(f"⚠️ 요금 변환 실패: {adult_fare}")

        # ✅ 해당 날짜에 최소 요금이 존재하면 저장, 없으면 None
        transformed_data.append({
            "date": date,
            "lowest_fare": min_fare if min_fare != float('inf') else None
        })

    logging.info(f"🟢 변환된 데이터 (각 날짜별 최소 요금): {transformed_data}")
    return transformed_data

from google.cloud import bigquery

def upload_to_bigquery(**kwargs):
    """
    변환된 데이터를 BigQuery에 업로드하며, 동일한 `date` 값이 있으면 덮어쓰는 기능 추가
    """
    bigquery_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
    
    # 변환된 데이터를 XCom에서 가져오기
    transformed_data = kwargs['ti'].xcom_pull(task_ids='fetch_transform_data')

    if not transformed_data:
        print("🔴 No data to insert into BigQuery. 변환된 데이터가 없습니다!")
        return

    print(f"🟢 BigQuery에 업로드할 데이터: {transformed_data}")  # 디버깅 로그 추가

    # BigQuery Client 가져오기
    client = bigquery_hook.get_client()

    table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET_NAME}.{BQ_TABLE_NAME}"

    # ✅ 테이블 존재 여부 확인 및 생성
    try:
        client.get_table(table_id)  # 테이블이 존재하는지 확인
    except Exception:
        print(f"[INFO] {BQ_TABLE_NAME} 테이블이 존재하지 않음, 생성 중...")
        schema = [
            bigquery.SchemaField("date", "STRING"),
            bigquery.SchemaField("lowest_fare", "INTEGER"),
        ]
        table = bigquery.Table(table_id, schema=schema)
        client.create_table(table)  # 테이블 생성
        print(f"[INFO] {BQ_TABLE_NAME} 테이블 생성 완료!")

    # ✅ 변환된 데이터를 임시 테이블에 삽입
    temp_table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET_NAME}.temp_{BQ_TABLE_NAME}"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",  # 임시 테이블을 덮어쓰기
        schema=[
            bigquery.SchemaField("date", "STRING"),
            bigquery.SchemaField("lowest_fare", "INTEGER"),
        ],
    )

    job = client.load_table_from_json(transformed_data, temp_table_id, job_config=job_config)
    job.result()  # 완료 대기
    print(f"✅ 임시 테이블 {temp_table_id}에 데이터 로드 완료!")

    # ✅ MERGE 쿼리 실행 (기존 데이터 업데이트, 새로운 데이터 삽입)
    merge_query = f"""
    MERGE `{table_id}` AS target
    USING `{temp_table_id}` AS source
    ON target.date = source.date
    WHEN MATCHED THEN
        UPDATE SET target.lowest_fare = source.lowest_fare
    WHEN NOT MATCHED THEN
        INSERT (date, lowest_fare) VALUES (source.date, source.lowest_fare)
    """
    query_job = client.query(merge_query)
    query_job.result()  # 완료 대기
    print("✅ MERGE 쿼리 실행 완료! 기존 데이터 업데이트 및 새로운 데이터 삽입 완료.")

    # ✅ 임시 테이블 삭제 (선택 사항)
    client.delete_table(temp_table_id, not_found_ok=True)
    print(f"🗑️ 임시 테이블 {temp_table_id} 삭제 완료!")


# DAG 정의
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(KST),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='naver_flight_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:
    gcs_task = PythonOperator(
        task_id='check_gcs',
        python_callable=check_gcs_connection
    )

    crawl_and_upload_task = PythonOperator(
        task_id="fetch_and_upload_flight_data",
        python_callable=fetch_flight_data_and_upload
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

    gcs_task >> crawl_and_upload_task >> fetch_transform_task >> upload_task
