from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery

from datetime import datetime, timedelta, timezone

import sys
import os
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

# ✅ 기존 GCS 연결 확인 방식 유지
def check_gcs_connection():
    hook = GCSHook()
    try:
        # ✅ 연결 확인을 위해 실제 업로드 시도
        hook.upload(
            bucket_name=GCS_BUCKET_NAME, 
            object_name="airflow_check/result.json", 
            data=json.dumps({"result": "success"})
        )
        logging.info("✅ GCS 연결 및 업로드 테스트 완료.")
    except Exception as e:
        hook.upload(
            bucket_name=GCS_BUCKET_NAME, 
            object_name="airflow_check/result.json", 
            data=json.dumps({"result": str(e)})
        )
        logging.error(f"🔴 GCS 연결 실패: {e}")

# ✅ XCom에 데이터 저장 추가 (추가된 부분)
def fetch_flight_data_and_upload(**kwargs):
    departure_airport = "ICN"
    arrival_airport = "KIX"

    crawler = NaverFlightCrawler()
    
    start_date = datetime.now(KST) + timedelta(days=10)
    end_date = start_date + timedelta(days=150)

    flight_data_list = []
    current_date = start_date
    while current_date <= end_date:
        departure_date = current_date.strftime("%Y%m%d")
        flight_data = crawler.fetch_flight_data(departure_airport, arrival_airport, departure_date)
        if flight_data:
            flight_data_list.append({"date": departure_date, "data": flight_data})
        current_date += timedelta(days=1)

    if flight_data_list:
        hook = GCSHook()
        hook.upload(
            bucket_name=GCS_BUCKET_NAME,
            object_name=GCS_OBJECT_NAME,
            data=json.dumps(flight_data_list, ensure_ascii=False, indent=4)
        )
        logging.info(f"✅ 데이터 GCS 업로드 완료: {GCS_OBJECT_NAME}")

# ✅ XCom 대신 GCS에 저장하는 방식으로 수정
def fetch_transform_data(**kwargs):
    gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
    raw_data = gcs_hook.download(GCS_BUCKET_NAME, GCS_OBJECT_NAME)
    data = json.loads(raw_data) if raw_data else []

    transformed_data = []
    for entry in data:
        date = entry.get("date")
        fares = entry.get("data", {}).get("fares", {})

        min_fare = float('inf')
        for flight_id, fare_info in fares.items():
            fare_types = fare_info.get("fare", {}).get("A01", [])
            for fare_option in fare_types:
                adult_fare = fare_option.get("Adult", {}).get("Fare")
                if adult_fare is not None:
                    try:
                        adult_fare = int(adult_fare)
                        min_fare = min(min_fare, adult_fare)
                    except ValueError:
                        logging.warning(f"⚠️ 요금 변환 실패: {adult_fare}")

        transformed_data.append({"date": date, "lowest_fare": min_fare if min_fare != float('inf') else None})

    logging.info(f"🟢 변환된 데이터: {transformed_data}")

    # ✅ 변환된 데이터를 GCS에 저장 (XCom 대신)
    transformed_gcs_path = f"naver_flight_data/transformed_flight_results.json"
    gcs_hook.upload(
        bucket_name=GCS_BUCKET_NAME,
        object_name=transformed_gcs_path,
        data=json.dumps(transformed_data, ensure_ascii=False, indent=4)
    )

    # ✅ XCom에는 변환 데이터가 저장된 GCS 경로만 저장
    kwargs['ti'].xcom_push(key='transformed_data_gcs_path', value=transformed_gcs_path)


# ✅ BigQuery 업로드 시 XCom 사용 방식 수정
def upload_to_bigquery(**kwargs):
    bigquery_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
    gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)

    # ✅ XCom에서 변환 데이터가 저장된 GCS 경로 가져오기
    transformed_gcs_path = kwargs['ti'].xcom_pull(task_ids='fetch_transform_data', key='transformed_data_gcs_path')
    
    if not transformed_gcs_path:
        logging.warning("🔴 No transformed data GCS path found in XCom.")
        return

    # ✅ 변환 데이터를 GCS에서 다운로드
    raw_transformed_data = gcs_hook.download(GCS_BUCKET_NAME, transformed_gcs_path)
    transformed_data = json.loads(raw_transformed_data) if raw_transformed_data else []

    if not transformed_data:
        logging.warning("🔴 No data to insert into BigQuery.")
        return

    client = bigquery_hook.get_client()
    table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET_NAME}.{BQ_TABLE_NAME}"
    temp_table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET_NAME}.temp_{BQ_TABLE_NAME}"

    # ✅ BigQuery 테이블 존재 여부 확인 및 생성
    try:
        client.get_table(table_id)
    except Exception:
        schema = [
            bigquery.SchemaField("date", "STRING"),
            bigquery.SchemaField("lowest_fare", "INTEGER"),
        ]
        client.create_table(bigquery.Table(table_id, schema=schema))

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    client.load_table_from_json(transformed_data, temp_table_id, job_config=job_config).result()

    merge_query = f"""
    MERGE `{table_id}` AS target
    USING `{temp_table_id}` AS source
    ON target.date = source.date
    WHEN MATCHED THEN
        UPDATE SET target.lowest_fare = source.lowest_fare
    WHEN NOT MATCHED THEN
        INSERT (date, lowest_fare) VALUES (source.date, source.lowest_fare)
    """
    client.query(merge_query).result()
    client.delete_table(temp_table_id, not_found_ok=True)

    logging.info("✅ BigQuery 데이터 업로드 및 MERGE 완료.")

# ✅ DAG 설정 변경
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 1, tzinfo=KST),  # ✅ start_date를 고정된 과거 날짜로 설정 (수정됨)
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='naver_flight_pipeline',
    default_args=default_args,
    schedule_interval='0 */12 * * *',
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

    # ✅ provide_context 제거 (Airflow 2.x에서는 불필요)
    fetch_transform_task = PythonOperator(
        task_id="fetch_transform_data",
        python_callable=fetch_transform_data
    )

    upload_task = PythonOperator(
        task_id="upload_to_bigquery",
        python_callable=upload_to_bigquery
    )

    gcs_task >> crawl_and_upload_task >> fetch_transform_task >> upload_task