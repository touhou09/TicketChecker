import json
import pandas as pd
import logging

# example.json 파일 경로
EXAMPLE_JSON_PATH = "example.json"

def test_fetch_transform_data_local():
    with open(EXAMPLE_JSON_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    transformed_data = []

    for entry in data:
        date = entry.get("date")
        fares = entry.get("data", {}).get("fares", {})
        schedules = entry.get("data", {}).get("schedules", [])

        for flight_id, flight_info in fares.items():
            schedule_detail = schedules[0].get(flight_id, {}).get("detail", [{}])[0]
            airline_code = schedule_detail.get("av", "")
            dep_time = schedule_detail.get("sdt", "")
            arr_time = schedule_detail.get("edt", "")

            fare_list = flight_info.get("fare", {}).get("A01", [])
            if not fare_list:
                continue

            adult_info = fare_list[0].get("Adult", {})
            try:
                fare = int(adult_info.get("Fare", 0))
                tax = int(adult_info.get("Tax", 0))
                qcharge = int(adult_info.get("QCharge", 0))
                total_price = fare + tax + qcharge
            except Exception as e:
                logging.warning(f"🔴 가격 파싱 오류: {e}")
                continue

            transformed_data.append({
                "flight_id": flight_id,
                "date": date,
                "airline": airline_code,
                "price": total_price,
                "departure_time": dep_time,
                "arrival_time": arr_time,
            })

    df = pd.DataFrame(transformed_data)
    print("✅ 변환된 데이터 미리보기:")
    print(df.head(10).to_string(index=False))

    # Optional: 파일로 저장
    df.to_json("transformed_flight_results_test.json", orient="records", force_ascii=False, indent=4)

if __name__ == "__main__":
    test_fetch_transform_data_local()
