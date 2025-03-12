import requests
import pandas as pd
import time
from openpyxl import load_workbook
import os
import concurrent.futures
import threading
import random
from queue import Queue

# 스레드 안전성을 위한 lock 객체
excel_lock = threading.Lock()

def fetch_user_posts(api_key, username, max_posts=48):
    headers = {
        "x-rapidapi-key": api_key,
        "x-rapidapi-host": "instagram-scraper-api2.p.rapidapi.com"
    }
    
    all_posts = []
    
    # 먼저 reels API로 시도
    reels_url = "https://instagram-scraper-api2.p.rapidapi.com/v1/reels"
    current_pagination_token = None
    retry_count = 0
    max_retries = 3
    
    print(f"Fetching reels for user {username}")
    while len(all_posts) < max_posts:
        try:
            querystring = {
                "username_or_id_or_url": username,
                "pagination_token": current_pagination_token
            }
            
            response = requests.get(reels_url, headers=headers, params=querystring)
            response.raise_for_status()
            data = response.json()
            
            if not data:
                print("No data received from Reels API")
                break
                
            posts = data.get('data', {}).get('items', [])
            if not posts:
                print("No reels found in response")
                break
                
            all_posts.extend(posts)
            print(f"Fetched {len(posts)} reels. Total: {len(all_posts)}/{max_posts}")
            
            if len(all_posts) >= max_posts:
                break
                
            current_pagination_token = data.get('pagination_token')
            if not current_pagination_token:
                print(f"No more reels available for user {username}")
                break
                
            time.sleep(2)
            retry_count = 0
            
        except Exception as e:
            retry_count += 1
            print(f"Error fetching reels for {username} (Attempt {retry_count}/{max_retries}): {str(e)}")
            if retry_count >= max_retries:
                break
            time.sleep(2 ** retry_count)
    
    return {'data': {'items': all_posts[:max_posts]}}

def save_data_to_excel(data, filename):
    """스레드 안전한 데이터 저장 함수"""
    df = pd.DataFrame(data)
    
    with excel_lock:  # 파일 접근 시 lock 사용
        try:
            # 기존 파일이 있고 손상되었는지 확인
            if os.path.exists(filename):
                try:
                    existing_df = pd.read_excel(filename, sheet_name="Data")
                except Exception as e:
                    print(f"기존 파일이 손상되었습니다. 새로운 파일을 생성합니다: {e}")
                    existing_df = pd.DataFrame({col: pd.Series(dtype=str) for col in df.columns})
            else:
                existing_df = pd.DataFrame({col: pd.Series(dtype=str) for col in df.columns})

            # 데이터 타입 변환
            df = df.astype(str)
            existing_df = existing_df.astype(str)

            # 중복 제거 (User ID가 있는 경우)
            if "ID" in df.columns and "ID" in existing_df.columns:
                existing_ids = set(existing_df["ID"])
                df = df[~df["ID"].isin(existing_ids)]

            # 새로운 데이터 추가
            if not df.empty:
                final_df = pd.concat([existing_df, df], ignore_index=True)
                with pd.ExcelWriter(filename, engine='openpyxl', mode='w') as writer:
                    final_df.to_excel(writer, sheet_name="Data", index=False)
                print(f"{len(df)} new rows added to {filename}.")
            else:
                print("No new data to add. All entries are duplicates.")

        except FileNotFoundError:
            # 파일이 없을 경우 새로운 파일 생성
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name="Data")

def process_user_data(username, api_key):
    """단일 사용자의 데이터를 처리하는 함수"""
    try:
        print(f"\nProcessing data for user: {username}")
        
        posts_file = f'instagram_posts_{username}.xlsx'
        posts_data = fetch_user_posts(api_key, username, max_posts=48)
        
        if not posts_data or 'data' not in posts_data:
            print(f"Failed to fetch posts for user {username}")
            return False
        
        posts = posts_data['data'].get('items', [])
        print(f"Retrieved {len(posts)} posts for user {username}")
        
        posts_processed = [{
            'post_code': post.get('code'),
            'taken_at': post.get('taken_at'),
            'media_type': post.get('media_type'),
            'like_count': post.get('like_count'),
            'comment_count': post.get('comment_count'),
            'caption': post.get('caption', {}).get('text') if post.get('caption') else None,
            'video_view_count': post.get('video_view_count'),
            'video_duration': post.get('video_duration'),
            'product_type': post.get('product_type'),
            'is_paid_partnership': post.get('is_paid_partnership'),
            'location': post.get('location', {}).get('name') if post.get('location') else None,
            'user_id': post.get('user', {}).get('id'),
            'username': post.get('user', {}).get('username'),
            'user_full_name': post.get('user', {}).get('full_name'),
            'user_is_private': post.get('user', {}).get('is_private'),
            'user_is_verified': post.get('user', {}).get('is_verified')
        } for post in posts]
        
        save_data_to_excel(posts_processed, posts_file)
        return True
        
    except Exception as e:
        print(f"Error processing user {username}: {str(e)}")
        return False

def process_user_with_retry(username, api_key, max_retries=3):
    """재시도 로직이 포함된 사용자 처리 함수"""
    retry_count = 0
    while retry_count < max_retries:
        try:
            if process_user_data(username, api_key):
                print(f"Successfully processed user: {username}")
                return True
            retry_count += 1
        except Exception as e:
            retry_count += 1
            wait_time = 2 ** retry_count
            print(f"Error processing user {username} (attempt {retry_count}/{max_retries}): {str(e)}")
            print(f"Waiting {wait_time} seconds before retry...")
            time.sleep(wait_time)
    
    print(f"Failed to process user {username} after {max_retries} attempts")
    return False

def process_users_parallel(file_path, api_key, max_workers=3):
    """멀티스레드로 여러 사용자를 동시에 처리하는 함수"""
    # 파일 읽기
    if file_path.endswith('.csv'):
        users_df = pd.read_csv(file_path)
    elif file_path.endswith('.xlsx'):
        users_df = pd.read_excel(file_path)
    else:
        print("지원되지 않는 파일 형식입니다. CSV 또는 엑셀 파일을 사용하세요.")
        return

    if 'user_name' not in users_df.columns:
        print("파일에 'user_name' 열이 포함되어 있어야 합니다.")
        return

    usernames = users_df['user_name'].tolist()
    results = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 각 사용자에 대해 별도의 스레드에서 처리
        future_to_username = {
            executor.submit(process_user_with_retry, username, api_key): username 
            for username in usernames
        }

        for future in concurrent.futures.as_completed(future_to_username):
            username = future_to_username[future]
            try:
                success = future.result()
                results.append({
                    'username': username,
                    'status': 'success' if success else 'failed'
                })
            except Exception as e:
                print(f"Exception occurred while processing {username}: {str(e)}")
                results.append({
                    'username': username,
                    'status': 'error',
                    'error_message': str(e)
                })

    # 결과 저장
    results_df = pd.DataFrame(results)
    results_df.to_excel('crawling_results.xlsx', index=False)
    print("\n크롤링 완료. 결과가 crawling_results.xlsx 파일에 저장되었습니다.")

if __name__ == "__main__":
    api_key = "your_api_key_here"  # API 키를 입력하세요
    file_path = "instagram_users.xlsx"  # 사용자 목록이 있는 파일 경로
    max_workers = 3  # 동시에 실행할 최대 스레드 수
    
    process_users_parallel(file_path, api_key, max_workers) 