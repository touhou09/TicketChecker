import json
import pandas as pd

EXAMPLE_JSON_PATH = "/mnt/c/Users/Admin/projects/TicketChecker/airflow/test/example.json"

with open(EXAMPLE_JSON_PATH, "r", encoding="utf-8") as f:
    data = json.load(f)

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

        # ✅ 마지막 4자리만 추출 (HHMM)
        departure_time = departure_time_raw[-4:] if len(departure_time_raw) >= 12 else ""
        arrival_time = arrival_time_raw[-4:] if len(arrival_time_raw) >= 12 else ""

        fare_info_list = fares.get(flight_id, {}).get("fare", {}).get("A01", [])
        if not fare_info_list:
            continue

        for fare in fare_info_list:
            adult_fare = fare.get("Adult", {})
            try:
                total_price = (
                    int(adult_fare.get("Fare", 0)) +
                    int(adult_fare.get("Tax", 0)) +
                    int(adult_fare.get("QCharge", 0))
                )
            except Exception as e:
                print(f"⚠️ 요금 변환 오류: {e}")
                continue

            all_flight_info.append({
                "flight_id": flight_id,
                "date": date,
                "airline": airline_name,
                "price": total_price,
                "departure_time": departure_time,  # ✅ HHMM만 저장
                "arrival_time": arrival_time       # ✅ HHMM만 저장
            })

# ✅ 결과 저장
df = pd.DataFrame(all_flight_info)
df.to_json("all_flights_named_shorttime.json", orient="records", force_ascii=False, indent=4)

print("✅ 항공사명 및 시간(HHMM)만 포함된 데이터가 'all_flights_named_shorttime.json'에 저장되었습니다.")
