import json

# JSON 파일 열기
with open('records.json', 'r', encoding='utf-8-sig') as f:
    data = json.load(f)

# username 값들을 저장할 리스트 초기화
usernames = []

# 각 레코드를 반복하며 username 값을 찾음
for record in data:
    start_properties = record.get('path', {}).get('start', {}).get('properties', {})
    if 'username' in start_properties:
        usernames.append(start_properties['username'])

# username 값들 출력
for username in usernames:
    print(username)