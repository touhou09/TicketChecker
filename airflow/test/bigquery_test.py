import json
import pandas as pd
from datetime import datetime

# ✅ 파일 경로 설정 (모든 파일이 같은 폴더에 있다고 가정)
existing_file_path = "/mnt/c/Users/Admin/projects/TicketChecker/airflow/test/example.json"
new_file_path = "/mnt/c/Users/Admin/projects/TicketChecker/airflow/test/cleaned_bigquery_data.json"


# 기존 데이터 로드
with open(existing_file_path, "r", encoding="utf-8") as file:
    existing_data = json.load(file)

# 신규 데이터 로드
with open(new_file_path, "r", encoding="utf-8") as file:
    new_data = json.load(file)
import json
import pandas as pd

# 데이터프레임 변환
existing_df = pd.DataFrame(existing_data)
new_df = pd.DataFrame(new_data)

# 기존 데이터 중 NULL 값 제거
existing_df = existing_df.dropna(subset=['lowest_fare'])
new_df = new_df.dropna(subset=['lowest_fare'])

# MERGE 로직: 새로운 데이터가 기존 데이터와 다르면 업데이트, 동일하면 유지
merged_df = pd.concat([existing_df.set_index('date'), new_df.set_index('date')], axis=0)
merged_df = merged_df[~merged_df.index.duplicated(keep='last')].reset_index()

# ✅ 현재 날짜 이전 데이터 삭제 (수정된 부분)
today_str = pd.to_datetime("today").strftime('%Y%m%d')
final_df = merged_df[merged_df['date'] >= today_str]  # 기존 코드에서 final_df['date']를 merged_df['date']로 변경

# 결과 JSON 파일로 저장
output_file = "merged_bigquery_data.json"
final_df.to_json(output_file, orient="records", lines=True, force_ascii=False)

# 저장된 파일 경로 반환
print(f"✅ 최종 병합된 JSON 파일 저장 완료: {output_file}")
