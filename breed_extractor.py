import requests
from bs4 import BeautifulSoup
import pandas as pd

# 빈 데이터프레임 생성
df = pd.DataFrame(columns=["Breed"])

for pagenum in range(1, 11):
    url = f"https://www.royalcanin.com/kr/dogs/breeds?page={pagenum}"

    # 웹 페이지 요청
    response = requests.get(url)
    response.raise_for_status()  # 요청 성공 확인

    # HTML 파싱
    soup = BeautifulSoup(response.text, "html.parser")

    # 모든 h3 태그 텍스트 추출
    h3_texts = [h3.get_text(strip=True) for h3 in soup.find_all("h3")]

    # 페이지의 h3 텍스트를 데이터프레임에 추가
    df = pd.concat([df, pd.DataFrame(h3_texts, columns=["Breed"])], ignore_index=True)

# 데이터프레임을 엑셀 파일로 저장
excel_file_path = "breeds.xlsx"
df.to_excel(excel_file_path, index=False)

print(f"모든 페이지의 결과가 하나의 엑셀 파일로 저장되었습니다: {excel_file_path}")