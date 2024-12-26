import json

# JSON 파일 열기
with open('dataset-instagram-scraper.json', 'r') as f:
    data = json.load(f)

# 각 객체에서 'timestamp', 'taggedUsers', 'ownerUsername'만 유지
filtered_data = [{k: v for k, v in item.items() if k in ['timestamp', 'taggedUsers', 'ownerUsername']} for item in data]

# 'ownerUsername'과 'timestamp' 필드를 기준으로 데이터 정렬
sorted_data = sorted(filtered_data, key=lambda x: (x.get('ownerUsername', ''), x.get('timestamp', '')), reverse=True)

# 결과를 새 JSON 파일에 저장
with open('extracted_data.json', 'w') as f:
    json.dump(sorted_data, f, indent=4)