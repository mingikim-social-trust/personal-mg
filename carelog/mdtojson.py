import json
import os

def convert_md_to_json(md_file_path, json_file_path):
    try:
        # 마크다운 파일 읽기
        with open(md_file_path, 'r', encoding='utf-8') as file:
            content = file.read()
        
        # 줄바꿈을 \n으로 변환
        formatted_content = content.replace('\r\n', '\n').replace('\n', '\n')
        
        # JSON 객체 생성
        json_data = {
            "content": formatted_content
        }
        
        # JSON 파일로 저장
        with open(json_file_path, 'w', encoding='utf-8') as file:
            json.dump(json_data, file, ensure_ascii=False, indent=2)
            
        print(f"변환 완료: {json_file_path}")
        
    except Exception as e:
        print(f"오류 발생: {str(e)}")

if __name__ == "__main__":
    # 현재 디렉토리의 모든 .md 파일 처리
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    for filename in os.listdir(current_dir):
        if filename.endswith('.md'):
            md_path = os.path.join(current_dir, filename)
            json_path = os.path.join(current_dir, filename.replace('.md', '.json'))
            convert_md_to_json(md_path, json_path)


convert_md_to_json('개인정보처리방침 136d5e8318f880bd8619fcd3fa56bbd8.md', 'records.json')
