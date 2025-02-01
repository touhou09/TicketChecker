from airflow.providers.google.cloud.hooks.gcs import GCSHook
import json

# 항공사 및 공항 코드 매핑
AIRLINE_CODES = {
    "JL": "일본 항공",
    "TW": "티웨이항공",
    "7C": "제주항공",
    "NH": "전일본공수",
    "LJ": "진에어",
    "OZ": "아시아나항공",
    "KE": "대한항공",
    "ZE": "이스타항공",
    "RS": "에어서울"
}

AIRPORT_CODES = {
    "KIX": "오사카 간사이 국제공항",
    "ICN": "서울 인천국제공항"
}

def process_json(gcs_file_path):
    
    
    for entry in flight_data_list:
        date = entry["date"]
        flights = entry["data"]

        if not isinstance(flights, list):
            flights = [flights] if isinstance(flights, dict) else []

        min_fare = float("inf")
        best_flights = []

        for flight in flights:
            fare = flight.get("lowest_price_adult")
            if fare is not None and fare < min_fare:
                min_fare = fare
                best_flights = [flight]
            elif fare == min_fare:
                best_flights.append(flight)

        if best_flights:
            flight_info.append({
                "date": date,
                "lowest_price_adult": min_fare,
                "airlines": [AIRLINE_CODES.get(f["airline"], f["airline"]) for f in best_flights],
                "departure_times": [f["departure_time"] for f in best_flights],
                "arrival_times": [f["arrival_time"] for f in best_flights],
                "departure_airports": [AIRPORT_CODES.get(f["departure_airport"], f["departure_airport"]) for f in best_flights],
                "arrival_airports": [AIRPORT_CODES.get(f["arrival_airport"], f["arrival_airport"]) for f in best_flights]
            })

    return flight_info
