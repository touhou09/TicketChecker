from datetime import datetime
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

    def fetch_travel_keys(self, departure, arrival, departure_date, trip_type="RT"):
        referer = self.build_referer(departure, arrival, departure_date)
        print(f"[DEBUG] Referer URL: {referer}")  # 디버깅용 출력
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
                "stayLength": "",
                "trip": trip_type,
                "galileoKey": "",
                "travelBizKey": "",
            },
            "query": 'query getInternationalList($trip: InternationalList_TripType!, $itinerary: [InternationalList_itinerary]!, $adult: Int = 1, $child: Int = 0, $infant: Int = 0, $fareType: InternationalList_CabinClass!, $where: InternationalList_DeviceType = pc, $isDirect: Boolean = false, $stayLength: String, $galileoKey: String, $galileoFlag: Boolean = true, $travelBizKey: String, $travelBizFlag: Boolean = true) {\n  internationalList(\n    input: {trip: $trip, itinerary: $itinerary, person: {adult: $adult, child: $child, infant: $infant}, fareType: $fareType, where: $where, isDirect: $isDirect, stayLength: $stayLength, galileoKey: $galileoKey, galileoFlag: $galileoFlag, travelBizKey: $travelBizKey, travelBizFlag: $travelBizFlag}\n  ) {\n    galileoKey\n    travelBizKey\n  }\n}',
        }

        print(f"[DEBUG] Travel Keys Payload: {json.dumps(payload, indent=4, ensure_ascii=False)}")

        try:
            response = requests.post(self.base_url, json=payload, headers=self.headers)
            print(f"[DEBUG] HTTP 응답 상태 코드: {response.status_code}")
            print(f"[DEBUG] HTTP 응답 본문: {response.text}")
            response.raise_for_status()
            response_data = response.json()
            print(f"[DEBUG] Travel Keys Response: {json.dumps(response_data, indent=4, ensure_ascii=False)}")
            
            travel_biz_key = response_data["data"]["internationalList"].get("travelBizKey", "")
            galileo_key = response_data["data"]["internationalList"].get("galileoKey", "")
            return travel_biz_key, galileo_key
        except requests.exceptions.RequestException as e:
            print(f"[ERROR] HTTP 요청 오류: {e}")
        except KeyError as e:
            print(f"[ERROR] 응답 데이터에서 키 오류: {e}")
        return None, None

    def fetch_flight_data(self, departure, arrival, departure_date):
        travel_biz_key, galileo_key = self.fetch_travel_keys(departure, arrival, departure_date, trip_type="OW")

        if not travel_biz_key or not galileo_key:
            print("[ERROR] travelBizKey 또는 galileoKey를 가져오는 데 실패했습니다.")
            return None

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
                "itinerary": [
                    {"departureAirport": departure, "arrivalAirport": arrival, "departureDate": departure_date}
                ],
                "trip": "OW",
                "galileoKey": galileo_key,
                "travelBizKey": travel_biz_key,
            },
            "query": """
                query getInternationalList(
                    $trip: InternationalList_TripType!, 
                    $itinerary: [InternationalList_itinerary]!, 
                    $adult: Int = 1, 
                    $child: Int = 0, 
                    $infant: Int = 0, 
                    $fareType: InternationalList_CabinClass!, 
                    $where: InternationalList_DeviceType = pc, 
                    $isDirect: Boolean = false, 
                    $galileoKey: String, 
                    $galileoFlag: Boolean = true, 
                    $travelBizKey: String, 
                    $travelBizFlag: Boolean = true
                ) {
                internationalList(
                    input: {
                    trip: $trip, 
                    itinerary: $itinerary, 
                    person: {
                        adult: $adult, 
                        child: $child, 
                        infant: $infant
                    }, 
                    fareType: $fareType, 
                    where: $where, 
                    isDirect: $isDirect, 
                    galileoKey: $galileoKey, 
                    galileoFlag: $galileoFlag, 
                    travelBizKey: $travelBizKey, 
                    travelBizFlag: $travelBizFlag
                    }
                ) {
                    galileoKey
                    travelBizKey
                    results {
                        airlines
                        airports
                        fareTypes
                        schedules
                        fares
                        errors
                        carbonEmissionAverage {
                            directFlightCarbonEmissionItineraryAverage
                            directFlightCarbonEmissionAverage
                        }
                    }
                }
                }
                """
        }

        print(f"[DEBUG] Flight Data Payload: {json.dumps(payload, indent=4, ensure_ascii=False)}")

        try:
            response = requests.post(self.base_url, json=payload, headers=self.headers)
            print(f"[DEBUG] HTTP 응답 상태 코드: {response.status_code}")
            print(f"[DEBUG] HTTP 응답 본문: {response.text}")
            response.raise_for_status()
            response_data = response.json()
            print(f"[DEBUG] Flight Data Response: {json.dumps(response_data, indent=4, ensure_ascii=False)}")
            
            results = response_data["data"]["internationalList"].get("results", [])
            if not results:
                print("[INFO] 항공편 데이터가 없습니다.")
            return results
        except requests.exceptions.RequestException as e:
            if e.response is not None:
                print(f"[ERROR] HTTP 응답 상태 코드: {e.response.status_code}")
                print(f"[ERROR] HTTP 응답 본문: {e.response.text}")
            else:
                print(f"[ERROR] HTTP 요청 오류: {e}")
        except KeyError as e:
            print(f"[ERROR] 응답 데이터에서 키 오류: {e}")
        return None

if __name__ == "__main__":

    # 크롤러 인스턴스 생성
    crawler = NaverFlightCrawler()

    # 검색 파라미터
    departure_airport = "ICN"  # 인천
    arrival_airport = "KIX"    # 오사카 간사이
    departure_date = "20250310"  # 출발 날짜

    # 크롤링 실행
    results = crawler.fetch_flight_data(departure_airport, arrival_airport, departure_date)

    # 결과 출력
    if results:
        with open("flight_results.json", "w", encoding="utf-8") as f:
            json.dump(results, f, indent=4, ensure_ascii=False)
        print("[INFO] 결과가 flight_results.json 파일로 저장되었습니다.")
    else:
        print("항공편 데이터를 가져오는 데 실패했습니다.")
