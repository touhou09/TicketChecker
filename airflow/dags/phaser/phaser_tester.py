import json
import pandas as pd
from collections import defaultdict

# flight_results.json 파일 로드
file_path = "flight_results.json"
with open(file_path, "r", encoding="utf-8") as f:
    data = json.load(f)

# 항공권 데이터 추출
schedules = data["schedules"]

# 날짜별 최저가 항공권 정보 추출
flight_info = []
for flight_id, flight_data in schedules[0].items():
    details = flight_data["detail"][0]  # 첫 번째 세그먼트 정보 가져오기
    date = details["sdt"][:8]  # 출발 날짜 (YYYYMMDD)
    airline_code = details["av"]  # 항공사 코드
    departure_time = details["sdt"][8:]  # 출발 시간 (HHMM)
    arrival_time = details["edt"][8:]  # 도착 시간 (HHMM)

    # 해당 항공편의 요금 정보 찾기
    fare_info = data["fares"].get(flight_id, {}).get("fare", {}).get("A01", [])
    if fare_info:
        min_fare = min(int(f["Adult"]["Fare"]) for f in fare_info if "Adult" in f)
    else:
        min_fare = None

    flight_info.append({
        "date": date,
        "lowest_price_adult": min_fare,
        "airline": airline_code,
        "departure_time": departure_time,
        "arrival_time": arrival_time
    })

# 날짜별 최저가 항공권 그룹화
lowest_flight_per_day = defaultdict(lambda: {"lowest_price_adult": float("inf"), "airlines": [], "departure_times": [], "arrival_times": []})

for flight in flight_info:
    date = flight["date"]
    price = flight["lowest_price_adult"]

    if price is None:
        continue

    # 현재 저장된 최저가보다 낮으면 갱신
    if price < lowest_flight_per_day[date]["lowest_price_adult"]:
        lowest_flight_per_day[date] = {
            "lowest_price_adult": price,
            "airlines": [flight["airline"]],
            "departure_times": [flight["departure_time"]],
            "arrival_times": [flight["arrival_time"]],
        }
    # 동일한 최저가라면 리스트에 추가
    elif price == lowest_flight_per_day[date]["lowest_price_adult"]:
        lowest_flight_per_day[date]["airlines"].append(flight["airline"])
        lowest_flight_per_day[date]["departure_times"].append(flight["departure_time"])
        lowest_flight_per_day[date]["arrival_times"].append(flight["arrival_time"])

# 리스트 형식으로 변환
lowest_flight_list = [{"date": date, **info} for date, info in lowest_flight_per_day.items()]

# JSON 파일로 저장
output_file_path = "flight_lowest_price_grouped.json"
with open(output_file_path, "w", encoding="utf-8") as f:
    json.dump(lowest_flight_list, f, indent=4, ensure_ascii=False)

# 결과 출력
print(f"최저가 항공권 데이터가 {output_file_path}에 저장되었습니다.")