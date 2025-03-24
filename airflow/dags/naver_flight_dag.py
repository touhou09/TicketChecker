from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery

from datetime import datetime, timedelta, timezone

import pandas as pd

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
BQ_TABLE_NAME = "flight_price"

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
    
    start_date = datetime.now(KST) + timedelta(days=7)
    end_date = start_date + timedelta(days=180)

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

    all_flight_info = []

    for entry in data:
        date = entry.get("date")
        schedules_list = entry.get("data", {}).get("schedules", [])
        fares = entry.get("data", {}).get("fares", {})
        airlines = entry.get("data", {}).get("airlines", {})

        if not schedules_list:
            continue

        schedules = schedules_list[0]

        for flight_id, flight_data in schedules.items():
            detail = flight_data.get("detail", [{}])[0]
            airline_code = detail.get("av", "")
            airline_name = airlines.get(airline_code, airline_code)

            departure_time_raw = detail.get("sdt", "")
            arrival_time_raw = detail.get("edt", "")
            departure_time = departure_time_raw[-4:] if len(departure_time_raw) >= 12 else None
            arrival_time = arrival_time_raw[-4:] if len(arrival_time_raw) >= 12 else None

            fare_info_list = fares.get(flight_id, {}).get("fare", {}).get("A01", [])
            if not fare_info_list:
                continue

            for fare in fare_info_list:
                
                if fare.get("FareType") != "A01":
                    continue  # 👈 A01이 아닌 경우 스킵
                
                adult_fare = fare.get("Adult", {})

                try:
                    total_price = (
                        int(adult_fare.get("Fare", 0)) +
                        int(adult_fare.get("Tax", 0)) +
                        int(adult_fare.get("QCharge", 0))
                    )
                except Exception as e:
                    logging.warning(f"⚠️ 요금 변환 오류: {e}")
                    continue

                all_flight_info.append({
                    "flight_id": flight_id,
                    "date": date,
                    "airline": airline_name,
                    "price": total_price,
                    "departure_time": departure_time,
                    "arrival_time": arrival_time
                })

    df = pd.DataFrame(all_flight_info)

    # ✅ NULL 값 제거
    df = df.dropna(subset=["flight_id", "price", "departure_time", "arrival_time"])
    df = df.drop_duplicates(subset=["flight_id"])

    transformed_json = df.to_json(orient="records", force_ascii=False, indent=4)

    transformed_gcs_path = f"naver_flight_data/transformed_flight_results.json"
    gcs_hook.upload(
        bucket_name=GCS_BUCKET_NAME,
        object_name=transformed_gcs_path,
        data=transformed_json
    )

    logging.info(f"🟢 변환 완료 후 {len(df)}건 유지 (NULL 제거됨)")

    kwargs['ti'].xcom_push(key='transformed_data_gcs_path', value=transformed_gcs_path)

# ✅ Bigquery에 데이터 저장 (flight_id 기반으로 중복 데이터는 최신화)
def upload_to_bigquery(**kwargs):

    bigquery_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
    gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)

    transformed_gcs_path = kwargs['ti'].xcom_pull(task_ids='fetch_transform_data', key='transformed_data_gcs_path')
    if not transformed_gcs_path:
        logging.warning("🔴 XCom에서 변환 경로를 찾지 못했습니다.")
        return

    raw_transformed_data = gcs_hook.download(GCS_BUCKET_NAME, transformed_gcs_path)
    transformed_data = json.loads(raw_transformed_data) if raw_transformed_data else []

    if not transformed_data:
        logging.warning("🔴 BigQuery에 업로드할 데이터가 없습니다.")
        return

    client = bigquery_hook.get_client()
    table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET_NAME}.{BQ_TABLE_NAME}"
    temp_table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET_NAME}.temp_{BQ_TABLE_NAME}"

    schema = [
        bigquery.SchemaField("flight_id", "STRING"),
        bigquery.SchemaField("date", "STRING"),
        bigquery.SchemaField("airline", "STRING"),
        bigquery.SchemaField("price", "INTEGER"),
        bigquery.SchemaField("departure_time", "STRING"),
        bigquery.SchemaField("arrival_time", "STRING"),
    ]

    try:
        client.get_table(table_id)
    except Exception:
        client.create_table(bigquery.Table(table_id, schema=schema))

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", schema=schema)
    client.load_table_from_json(transformed_data, temp_table_id, job_config=job_config).result()

    # ✅ 최신 데이터 병합
    merge_query = f"""
    MERGE `{table_id}` AS target
    USING `{temp_table_id}` AS source
    ON target.flight_id = source.flight_id
    WHEN MATCHED THEN
      UPDATE SET
        target.airline = source.airline,
        target.price = source.price,
        target.departure_time = source.departure_time,
        target.arrival_time = source.arrival_time
    WHEN NOT MATCHED THEN
      INSERT (flight_id, date, airline, price, departure_time, arrival_time)
      VALUES (source.flight_id, source.date, source.airline, source.price, source.departure_time, source.arrival_time)
    """
    client.query(merge_query).result()

    # ✅ 과거 데이터 삭제
    cleanup_query = f"""
    DELETE FROM `{table_id}`
    WHERE PARSE_DATE('%Y%m%d', date) < DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    """
    client.query(cleanup_query).result()

    client.delete_table(temp_table_id, not_found_ok=True)

    logging.info("✅ MERGE 완료 및 이전 날짜 데이터 정리 완료")

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
    schedule_interval='0 */8 * * *',
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