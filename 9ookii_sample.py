from bs4 import BeautifulSoup

# # HTML 파일 열기
# with open('./9ookii_sample.html', 'r') as f:
#     contents = f.read()

# soup = BeautifulSoup(contents, 'html.parser')

# # 'a' 태그의 수를 세기
# a_tags = soup.find_all('img')
# print(len(a_tags))

# 파일 열기
with open('9ookii_sample.html', 'r', encoding='utf-8') as file:
    # 파일 읽기
    content = file.read()
    # '팔로우'의 출현 횟수 찾기
    follow_count = content.count('팔로우')
    # '팔로잉'의 출현 횟수 찾기
    following_count = content.count('팔로잉')

print('팔로우의 출현 횟수:', follow_count)
print('팔로잉의 출현 횟수:', following_count)