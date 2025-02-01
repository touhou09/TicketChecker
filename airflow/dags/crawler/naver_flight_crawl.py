from datetime import datetime, timedelta
import json
import requests

class NaverFlightCrawler:
    def __init__(self):
        self.base_url = "https://airline-api.naver.com/graphql"
        self.headers = {
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Accept": "*/*",
            "Origin": "https://flight.naver.com",
        }

    def build_referer(self, departure, arrival, departure_date):
        return f"https://flight.naver.com/flights/international/{departure}-{arrival}-{departure_date}?adult=1&fareType=Y"

    def fetch_flight_data(self, departure, arrival, departure_date):
        referer = self.build_referer(departure, arrival, departure_date)
        self.headers["Referer"] = referer

        payload = {
            "operationName": "getInternationalList",
            "variables": {
                "adult": 1,
                "child": 0,
                "infant": 0,
                "where": "pc",
                "isDirect": True,
                "galileoFlag": True,
                "travelBizFlag": True,
                "fareType": "Y",
                "itinerary": [{"departureAirport": departure, "arrivalAirport": arrival, "departureDate": departure_date}],
                "trip": "OW",
            },
            "query": """query getInternationalList($trip: InternationalList_TripType!, $itinerary: [InternationalList_itinerary]!, 
                        $adult: Int = 1, $child: Int = 0, $infant: Int = 0, $fareType: InternationalList_CabinClass!, 
                        $where: InternationalList_DeviceType = pc, $isDirect: Boolean = false) {
                internationalList(input: {trip: $trip, itinerary: $itinerary, person: {adult: $adult, child: $child, infant: $infant}, 
                fareType: $fareType, where: $where, isDirect: $isDirect}) {
                    results {
                        fares
                    }
                }
            }"""
        }

        try:
            response = requests.post(self.base_url, json=payload, headers=self.headers)
            response.raise_for_status()
            response_data = response.json()

            # [DEBUG] 응답 데이터 확인
            print(f"[DEBUG] 응답 데이터: {json.dumps(response_data, indent=4, ensure_ascii=False)}")

            return response_data.get("data", {}).get("internationalList", {}).get("results", [])
        except requests.exceptions.RequestException as e:
            print(f"[ERROR] HTTP 요청 오류: {e}")
        return None

    def get_min_fare(self, fares):
        if not fares:
            return None

        min_fare = float('inf')
        airline_code = None

        for flight_id, flight_info in fares.items():
            fare_details = flight_info.get("fare", {}).get("A01", [])
            for fare_option in fare_details:
                try:
                    total_fare = int(fare_option["Adult"]["Fare"]) + int(fare_option["Adult"]["Tax"]) + int(fare_option["Adult"]["QCharge"])
                    if total_fare < min_fare:
                        min_fare = total_fare
                        airline_code = fare_option.get("FareType", None)
                except (TypeError, ValueError, KeyError) as e:
                    print(f"[ERROR] 가격 데이터 오류: {e}")

        return {"비용": min_fare, "항공사": airline_code} if airline_code else None

    def collect_flight_data(self, departure, arrival):
        start_date = datetime.today()
        end_date = start_date + timedelta(days=180)  # 6개월 후

        summary_data = {}

        current_date = start_date
        while current_date <= end_date:
            departure_date = current_date.strftime("%Y%m%d")
            print(f"[INFO] {departure_date} 데이터 수집 중...")

            flight_data = self.fetch_flight_data(departure, arrival, departure_date)
            if not flight_data:
                print(f"[INFO] {departure_date}에 대한 데이터가 없습니다.")
                current_date += timedelta(days=1)
                continue

            fares = {}
            for result in flight_data:
                fares.update(result.get("fares", {}))

            if not fares:
                print(f"[INFO] {departure_date}에 대한 요금 정보가 없습니다.")
                current_date += timedelta(days=1)
                continue

            min_fare_info = self.get_min_fare(fares)
            if min_fare_info:
                summary_data[departure_date] = min_fare_info

            current_date += timedelta(days=1)

        return summary_data


if __name__ == "__main__":
    crawler = NaverFlightCrawler()

    departure_airport = "ICN"  # 인천
    arrival_airport = "KIX"    # 오사카 간사이

    results = crawler.collect_flight_data(departure_airport, arrival_airport)

    if results:
        with open("flight_summary.json", "w", encoding="utf-8") as f:
            json.dump(results, f, indent=4, ensure_ascii=False)
        print("[INFO] 최저가 항공편 정보가 flight_summary.json 파일로 저장되었습니다.")
    else:
        print("[INFO] 수집된 데이터가 없습니다.")
