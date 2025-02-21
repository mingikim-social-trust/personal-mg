import requests
import pandas as pd
import time
from openpyxl import load_workbook
import os
import zipfile

# 전역 상수 정의
MAX_COMMENTS_PER_POST = 2000  # 게시물당 최대 댓글 수
MAX_POSTS_PER_USER = 48      # 유저당 최대 게시물 수

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
    
    print(f"Completed fetching reels. Total reels: {len(all_posts)}")
    
    # reels로 충분하지 않은 경우 posts API로 추가 수집
    if len(all_posts) < max_posts:
        remaining_posts = max_posts - len(all_posts)
        print(f"Fetching additional {remaining_posts} posts using Posts API")
        
        posts_url = "https://instagram-scraper-api2.p.rapidapi.com/v1/posts"
        current_pagination_token = None
        retry_count = 0
        
        while len(all_posts) < max_posts:
            try:
                querystring = {
                    "username_or_id_or_url": username,
                    "pagination_token": current_pagination_token
                }
                
                response = requests.get(posts_url, headers=headers, params=querystring)
                response.raise_for_status()
                data = response.json()
                
                if not data:
                    break
                    
                posts = data.get('data', {}).get('items', [])
                if not posts:
                    break
                    
                # 중복 제거
                new_post_codes = {post.get('code') for post in posts}
                existing_post_codes = {post.get('code') for post in all_posts}
                unique_posts = [post for post in posts if post.get('code') not in existing_post_codes]
                
                all_posts.extend(unique_posts)
                print(f"Fetched {len(unique_posts)} additional posts. Total: {len(all_posts)}/{max_posts}")
                
                if len(all_posts) >= max_posts:
                    break
                    
                current_pagination_token = data.get('pagination_token')
                if not current_pagination_token:
                    break
                    
                time.sleep(2)
                retry_count = 0
                
            except Exception as e:
                retry_count += 1
                print(f"Error fetching posts for {username} (Attempt {retry_count}/{max_retries}): {str(e)}")
                if retry_count >= max_retries:
                    break
                time.sleep(2 ** retry_count)
    
    return {'data': {'items': all_posts[:max_posts]}}

def fetch_post_likers(api_key, post_code):
    url = "https://instagram-scraper-api2.p.rapidapi.com/v1/likes"
    querystring = {"code_or_id_or_url": post_code}
    headers = {
        "x-rapidapi-key": api_key,
        "x-rapidapi-host": "instagram-scraper-api2.p.rapidapi.com"
    }
    
    try:
        response = requests.get(url, headers=headers, params=querystring)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching likers for post {post_code}: {str(e)}")
        return None

def fetch_post_comments(api_key, post_code, pagination_token=None, max_retries=3, collected_count=0):
    url = "https://instagram-scraper-api2.p.rapidapi.com/v1/comments"
    headers = {
        "x-rapidapi-key": api_key,
        "x-rapidapi-host": "instagram-scraper-api2.p.rapidapi.com"
    }
    
    all_comments = []
    current_pagination_token = pagination_token
    retry_count = 0
    
    while True:
        # MAX_COMMENTS_PER_POST 이상 수집했으면 중단
        if collected_count + len(all_comments) >= MAX_COMMENTS_PER_POST:
            print(f"Reached {MAX_COMMENTS_PER_POST} comments limit. Stopping API calls.")
            return {'data': {'items': all_comments[:MAX_COMMENTS_PER_POST-collected_count]}}
        
        try:
            querystring = {
                "code_or_id_or_url": post_code,
                "pagination_token": current_pagination_token
            }
            
            response = requests.get(url, headers=headers, params=querystring)
            
            if response.status_code == 404:
                print(f"No comments available for post {post_code} (404 Not Found)")
                return {'data': {'items': all_comments}}
            
            response.raise_for_status()
            data = response.json()
            
            if response.status_code == 200:
                comments = data.get("data", {}).get("items", [])
                if not comments:
                    break
                
                # 남은 수집 가능한 개수 계산
                remaining = MAX_COMMENTS_PER_POST - (collected_count + len(all_comments))
                if len(comments) > remaining:
                    all_comments.extend(comments[:remaining])
                    print(f"Collected enough comments. Current total: {collected_count + len(all_comments)}")
                    break
                else:
                    all_comments.extend(comments)
                    print(f"Fetched {len(comments)} comments. Current total: {collected_count + len(all_comments)}")
                
                if collected_count + len(all_comments) >= MAX_COMMENTS_PER_POST:
                    break
                
                current_pagination_token = data.get("pagination_token")
                if not current_pagination_token:
                    break
                
                time.sleep(1)
                retry_count = 0
            else:
                break
            
        except requests.exceptions.RequestException as e:
            retry_count += 1
            if "404" in str(e):
                return {'data': {'items': all_comments}}
            
            print(f"Error fetching comments (Attempt {retry_count}/{max_retries}): {str(e)}")
            
            if retry_count >= max_retries:
                break
            
            time.sleep(2 ** retry_count)
            continue
    
    return {'data': {'items': all_comments}}

def save_data_to_excel(data, filename, sheet_name):
    df = pd.DataFrame(data)
    
    try:
        # 기존 파일이 있고 손상되었는지 확인
        if os.path.exists(filename):
            try:
                existing_df = pd.read_excel(filename, sheet_name=sheet_name)
            except Exception as e:
                print(f"기존 파일이 손상되었습니다. 새로운 파일을 생성합니다: {e}")
                existing_df = pd.DataFrame({col: pd.Series(dtype=str) for col in df.columns})
        else:
            existing_df = pd.DataFrame({col: pd.Series(dtype=str) for col in df.columns})

        # 숫자형 컬럼 식별
        numeric_columns = ['like_count', 'comment_count', 'video_view_count', 'video_duration', 
                         'comments_count', 'like_count']
        
        # NaN 값 처리
        for col in df.columns:
            if col in numeric_columns:
                # 숫자형 컬럼의 NaN을 0으로 변환
                df[col] = df[col].fillna(0)
                existing_df[col] = existing_df[col].fillna(0)
            else:
                # 문자형 컬럼의 NaN을 빈 문자열로 변환
                df[col] = df[col].fillna('')
                existing_df[col] = existing_df[col].fillna('')

        # 데이터 타입 변환
        for col in df.columns:
            if col in numeric_columns:
                # 숫자형 컬럼은 정수형으로 변환
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
                existing_df[col] = pd.to_numeric(existing_df[col], errors='coerce').fillna(0).astype(int)
            else:
                # 나머지는 문자열로 변환
                df[col] = df[col].astype(str).replace('nan', '')
                existing_df[col] = existing_df[col].astype(str).replace('nan', '')

        # 중복 제거 (post_code나 user_id가 있는 경우)
        if "post_code" in df.columns and "post_code" in existing_df.columns:
            existing_codes = set(existing_df["post_code"])
            df = df[~df["post_code"].isin(existing_codes)]
        elif "user_id" in df.columns and "user_id" in existing_df.columns:
            # 좋아요/댓글의 경우 post_code와 user_id 조합으로 중복 체크
            df['temp_key'] = df['post_code'] + '_' + df['user_id']
            existing_df['temp_key'] = existing_df['post_code'] + '_' + existing_df['user_id']
            existing_keys = set(existing_df['temp_key'])
            df = df[~df['temp_key'].isin(existing_keys)]
            df = df.drop('temp_key', axis=1)

        # 새로운 데이터 추가
        if not df.empty:
            final_df = pd.concat([existing_df, df], ignore_index=True)
            with pd.ExcelWriter(filename, engine='openpyxl', mode='w') as writer:
                final_df.to_excel(writer, sheet_name=sheet_name, index=False)
            print(f"{len(df)} new rows added to {filename}.")
        else:
            print("No new data to add. All entries are duplicates.")

    except FileNotFoundError:
        # 파일이 없을 경우 새로운 파일 생성
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)
        print(f"Created new file {filename} with {len(df)} rows.")
    except Exception as e:
        print(f"Error saving data to {filename}: {str(e)}")
        # 에러 발생 시 백업 파일 생성
        backup_filename = f"backup_{int(time.time())}_{filename}"
        df.to_excel(backup_filename, sheet_name=sheet_name, index=False)
        print(f"Backup file created: {backup_filename}")

def save_progress(post_code, pagination_token, comments):
    """진행 상황을 저장하는 함수"""
    progress_data = {
        'post_code': post_code,
        'pagination_token': pagination_token,
        'last_comment_id': comments[-1]['id'] if comments else None,
        'comments_count': len(comments)
    }
    
    filename = 'comment_crawling_progress.xlsx'
    df = pd.DataFrame([progress_data])
    
    try:
        if os.path.exists(filename):
            existing_df = pd.read_excel(filename)
            # 동일한 post_code가 있으면 업데이트
            existing_df = existing_df[existing_df['post_code'] != post_code]
            df = pd.concat([existing_df, df], ignore_index=True)
        
        df.to_excel(filename, index=False)
        print(f"Progress saved to {filename}")
    except Exception as e:
        print(f"Error saving progress: {str(e)}")

def load_progress(post_code):
    """저장된 진행 상황을 불러오는 함수"""
    filename = 'comment_crawling_progress.xlsx'
    if not os.path.exists(filename):
        return None
    
    try:
        df = pd.read_excel(filename)
        progress = df[df['post_code'] == post_code].to_dict('records')
        return progress[0] if progress else None
    except Exception as e:
        print(f"Error loading progress: {str(e)}")
        return None

def save_crawling_status(username, post_code, status, message='', comment_pagination_token=None, comment_count=0):
    """크롤링 상태를 로그 파일에 저장"""
    status_data = {
        'username': username,
        'post_code': post_code,
        'status': status,  # 'completed', 'likes_done', 'error', 'comments_in_progress'
        'timestamp': pd.Timestamp.now(),
        'message': message,
        'comment_pagination_token': comment_pagination_token,
        'collected_comments': comment_count
    }
    
    filename = 'crawling_status.xlsx'
    df = pd.DataFrame([status_data])
    
    try:
        if os.path.exists(filename):
            existing_df = pd.read_excel(filename)
            # 동일한 username과 post_code 조합이 있으면 업데이트
            existing_df = existing_df[~((existing_df['username'] == username) & 
                                      (existing_df['post_code'] == post_code))]
            df = pd.concat([existing_df, df], ignore_index=True)
        
        df.to_excel(filename, index=False)
    except Exception as e:
        print(f"Error saving crawling status: {str(e)}")

def get_crawling_status(username):
    """특정 유저의 크롤링 진행 상황을 확인"""
    filename = 'crawling_status.xlsx'
    if not os.path.exists(filename):
        return []  # 빈 딕셔너리 대신 빈 리스트 반환
    
    try:
        df = pd.read_excel(filename)
        user_status = df[df['username'] == username].sort_values('timestamp')
        if user_status.empty:
            return []  # 해당 유저의 상태가 없으면 빈 리스트 반환
        return user_status.to_dict('records')
    except Exception as e:
        print(f"Error loading crawling status: {str(e)}")
        return []  # 에러 발생 시에도 빈 리스트 반환

def process_user_data(username, api_key):
    try:
        # 유저의 크롤링 상태 확인
        crawling_status = get_crawling_status(username)
        if crawling_status:  # 상태가 있는 경우에만 completed 체크
            if all(status['status'] == 'completed' for status in crawling_status):
                print(f"User {username} is already completed. Skipping...")
                return
        else:
            print(f"No previous crawling status found for {username}. Starting new crawl...")
            
        posts_file = f'instagram_posts_{username}.xlsx'
        
        # 게시물 파일 존재 여부 확인
        if os.path.exists(posts_file):
            try:
                posts_df = pd.read_excel(posts_file)
                print(f"Found existing posts file with {len(posts_df)} posts")
                posts_processed = posts_df.to_dict('records')
                new_posts = posts_processed  # 기존 게시물 사용
            except Exception as e:
                print(f"Error reading existing posts file: {str(e)}")
                return
        else:
            # 게시물 파일이 없는 경우에만 새로 크롤링
            posts_data = fetch_user_posts(api_key, username, max_posts=48)
            if not posts_data or 'data' not in posts_data:
                save_crawling_status(username, '', 'error', 'Failed to fetch posts')
                return
            
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
            
            save_data_to_excel(posts_processed, posts_file, 'Posts')
            print(f"Saved {len(posts_processed)} posts to file")
            new_posts = posts_processed

        # 이전 크롤링 상태 확인
        completed_posts = {status['post_code'] for status in crawling_status 
                         if status['status'] == 'completed'}
        
        # 미완료된 게시물만 처리
        posts_to_process = [post for post in new_posts 
                           if post['post_code'] not in completed_posts]
        
        if not posts_to_process:
            print(f"All posts for {username} have been processed")
            return
        
        print(f"Found {len(posts_to_process)} posts to process")
        
        # 각 게시물의 좋아요와 댓글 데이터 수집
        for post in posts_to_process:
            try:
                post_code = post['post_code']
                print(f"\nProcessing post: {post_code}")
                
                try:
                    # 좋아요 데이터 수집
                    likers_data = fetch_post_likers(api_key, post_code)
                    if likers_data and 'data' in likers_data:
                        likers = likers_data['data'].get('items', [])
                        actual_like_count = len(likers)
                        
                        # 좋아요 수 업데이트
                        try:
                            df = pd.read_excel(posts_file)
                            current_like_count = df.loc[df['post_code'] == post_code, 'like_count'].values[0]
                            print(f"Current like count for post {post_code}: {current_like_count}")
                            
                            if current_like_count < actual_like_count:
                                df.loc[df['post_code'] == post_code, 'like_count'] = actual_like_count
                                df.to_excel(posts_file, index=False)
                                print(f"Updated like count for post {post_code}: {actual_like_count} (previous: {current_like_count})")
                            else:
                                print(f"No update needed for post {post_code}. Current: {current_like_count}, New: {actual_like_count}")
                        except Exception as e:
                            print(f"Error updating like count: {str(e)}")
                        
                        # 좋아요 데이터 저장
                        likers_processed = [{
                            'post_code': post_code,
                            'user_id': liker.get('id'),
                            'username': liker.get('username'),
                            'full_name': liker.get('full_name'),
                            'is_private': liker.get('is_private'),
                            'is_verified': liker.get('is_verified'),
                            'profile_url': f"https://www.instagram.com/{liker.get('username')}/" if liker.get('username') else ""
                        } for liker in likers]
                        
                        save_data_to_excel(likers_processed, f'instagram_likers_{username}.xlsx', 'Likers')
                        print(f"Saved {len(likers)} likes for post {post_code}")
                        save_crawling_status(username, post_code, 'likes_done')
                    
                    # 댓글 데이터 수집
                    post_status = next((status for status in crawling_status 
                                      if status['post_code'] == post_code), None)
                    
                    comment_pagination_token = None
                    collected_comments = 0
                    
                    if post_status:
                        if post_status['status'] == 'completed':
                            print(f"Comments already completed for post {post_code}")
                            continue
                        elif post_status['status'] == 'comments_in_progress':
                            token = post_status.get('comment_pagination_token')
                            # 'nan' 또는 None 값 처리
                            comment_pagination_token = None if pd.isna(token) or token == 'nan' else token
                            collected_comments = post_status.get('collected_comments', 0)
                            # 숫자가 float으로 저장된 경우 처리
                            collected_comments = int(collected_comments) if not pd.isna(collected_comments) else 0
                            
                            if comment_pagination_token:
                                print(f"Resuming comments from pagination token: {comment_pagination_token}")
                            print(f"Previously collected comments: {collected_comments}")
                    
                    while True:
                        try:
                            # API 호출 전에 남은 수집 가능한 댓글 수 계산
                            remaining_comments = MAX_COMMENTS_PER_POST - collected_comments
                            
                            if remaining_comments <= 0:
                                print(f"Already reached maximum comment limit ({MAX_COMMENTS_PER_POST}) for post {post_code}")
                                save_crawling_status(username, post_code, 'completed', 
                                                   comment_count=collected_comments)
                                break
                            
                            comments_data = fetch_post_comments(api_key, post_code, 
                                                                  pagination_token=comment_pagination_token,
                                                                  collected_count=collected_comments)
                            
                            if not comments_data or 'data' not in comments_data:
                                save_crawling_status(username, post_code, 'completed', 
                                                   comment_count=collected_comments)
                                break
                            
                            comments = comments_data['data'].get('items', [])
                            if not comments:
                                save_crawling_status(username, post_code, 'completed', 
                                                   comment_count=collected_comments)
                                break
                            
                            # 남은 수집 가능한 개수만큼만 처리
                            if collected_comments + len(comments) > MAX_COMMENTS_PER_POST:
                                comments = comments[:MAX_COMMENTS_PER_POST - collected_comments]
                            
                            # 실제로 처리할 댓글 수만 출력
                            print(f"Processing {len(comments)} comments for post {post_code}. Current total: {collected_comments}, Will be: {collected_comments + len(comments)}")
                            
                            comments_processed = [{
                                'post_code': post_code,
                                'comment_id': comment.get('id'),
                                'text': comment.get('text'),
                                'hashtags': ', '.join(comment.get('hashtags', [])),
                                'mentions': ', '.join(comment.get('mentions', [])),
                                'like_count': comment.get('like_count', 0),
                                'replied_to': comment.get('replied_to_comment_id', ''),
                                'is_owner': comment.get('is_created_by_media_owner', False),
                                'created_at': comment.get('created_at'),
                                'comment_type': comment.get('type', 0),
                                'user_id': comment.get('user', {}).get('id'),
                                'username': comment.get('user', {}).get('username', 'Unknown'),
                                'full_name': comment.get('user', {}).get('full_name', ''),
                                'is_private': comment.get('user', {}).get('is_private'),
                                'is_verified': comment.get('user', {}).get('is_verified')
                            } for comment in comments]
                            
                            if comments_processed:
                                save_data_to_excel(comments_processed, 
                                                 f'instagram_comments_{username}.xlsx', 
                                                 'Comments')
                                collected_comments += len(comments_processed)
                                print(f"Saved {len(comments_processed)} new comments. Total: {collected_comments}")
                            
                            # 최대 댓글 수에 도달하면 즉시 중단
                            if collected_comments >= MAX_COMMENTS_PER_POST:
                                print(f"Reached maximum limit of {MAX_COMMENTS_PER_POST} comments for post {post_code}")
                                save_crawling_status(username, post_code, 'completed', 
                                                   comment_count=collected_comments)
                                break
                            
                            # 다음 페이지 토큰 확인 및 진행 상황 저장
                            comment_pagination_token = comments_data.get('pagination_token')
                            if not comment_pagination_token:
                                print(f"No more comments to fetch for post {post_code}")
                                save_crawling_status(username, post_code, 'completed', 
                                                   comment_count=collected_comments)
                                break
                            else:
                                save_crawling_status(username, post_code, 'comments_in_progress',
                                                   comment_pagination_token=comment_pagination_token,
                                                   comment_count=collected_comments)
                                print(f"Saved progress with token: {comment_pagination_token}")
                            
                            time.sleep(1)
                        
                        except KeyboardInterrupt:
                            print("\nCrawling interrupted by user")
                            save_crawling_status(username, post_code, 'comments_in_progress',
                                               comment_pagination_token=comment_pagination_token,
                                               comment_count=collected_comments)
                            print(f"Progress saved. You can resume from post {post_code} with token: {comment_pagination_token}")
                            raise
                        except Exception as e:
                            print(f"Error in comments loop: {str(e)}")
                            save_crawling_status(username, post_code, 'error',
                                               message=str(e),
                                               comment_pagination_token=comment_pagination_token,
                                               comment_count=collected_comments)
                            break
                    
                except KeyboardInterrupt:
                    raise
                except Exception as e:
                    print(f"Error processing post {post_code}: {str(e)}")
                    continue
                
                time.sleep(1)
                
            except KeyboardInterrupt:
                raise
            except Exception as e:
                print(f"Error processing post {post_code}: {str(e)}")
                continue
        
    except KeyboardInterrupt:
        print("\nCrawling interrupted by user")
        print("Progress has been saved. You can resume later.")
        raise
    except Exception as e:
        print(f"Error in process_user_data: {str(e)}")

def main():
    api_key = "fb1be9caf7mshc1fea79903f370fp1c0b11jsn97070d82933d"
    input_file = "instagram_users.xlsx"
    
    try:
        users_df = pd.read_excel(input_file)
        usernames = users_df['username'].tolist()
        
        for username in usernames:
            try:
                print(f"\nProcessing data for user: {username}")
                
                # 크롤링 상태 확인 로직 제거 (process_user_data 내부에서 처리)
                process_user_data(username, api_key)
                time.sleep(2)
                
            except KeyboardInterrupt:
                print(f"\nCrawling interrupted while processing {username}")
                user_input = input("Do you want to continue with next user? (y/n): ")
                if user_input.lower() != 'y':
                    print("Exiting program")
                    break
                continue
            except Exception as e:
                print(f"Error processing user {username}: {str(e)}")
                continue
            
    except KeyboardInterrupt:
        print("\nProgram interrupted by user")
    except Exception as e:
        print(f"Error processing users: {str(e)}")
    finally:
        print("Program finished")

if __name__ == "__main__":
    main() 