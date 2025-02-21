import requests
import pandas as pd
import time
from openpyxl import load_workbook
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# 전역 상수 및 파일 I/O를 위한 락 생성
MAX_COMMENTS_PER_POST = 2000  # 게시물당 최대 댓글 수
MAX_POSTS_PER_USER = 48       # 유저당 최대 게시물 수
file_lock = Lock()

def fetch_user_posts(api_key, username, max_posts=48):
    headers = {
        "x-rapidapi-key": api_key,
        "x-rapidapi-host": "instagram-scraper-api2.p.rapidapi.com"
    }
    
    all_posts = []
    reels_url = "https://instagram-scraper-api2.p.rapidapi.com/v1/reels"
    current_pagination_token = None
    retry_count = 0
    max_retries = 3
    
    print(f"[{username}] Fetching reels...")
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
                print(f"[{username}] No data received from Reels API")
                break
                
            posts = data.get('data', {}).get('items', [])
            if not posts:
                print(f"[{username}] No reels found in response")
                break
                
            all_posts.extend(posts)
            print(f"[{username}] Fetched {len(posts)} reels. Total: {len(all_posts)}/{max_posts}")
            if len(all_posts) >= max_posts:
                break
                
            current_pagination_token = data.get('pagination_token')
            if not current_pagination_token:
                print(f"[{username}] No more reels available")
                break
                
            time.sleep(2)
            retry_count = 0
            
        except Exception as e:
            retry_count += 1
            print(f"[{username}] Error fetching reels (Attempt {retry_count}/{max_retries}): {str(e)}")
            if retry_count >= max_retries:
                break
            time.sleep(2 ** retry_count)
    
    print(f"[{username}] Completed fetching reels. Total reels: {len(all_posts)}")
    
    # 부족한 경우 Posts API 사용
    if len(all_posts) < max_posts:
        remaining_posts = max_posts - len(all_posts)
        print(f"[{username}] Fetching additional {remaining_posts} posts using Posts API")
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
                existing_post_codes = {post.get('code') for post in all_posts}
                unique_posts = [post for post in posts if post.get('code') not in existing_post_codes]
                all_posts.extend(unique_posts)
                print(f"[{username}] Fetched {len(unique_posts)} additional posts. Total: {len(all_posts)}/{max_posts}")
                if len(all_posts) >= max_posts:
                    break
                current_pagination_token = data.get('pagination_token')
                if not current_pagination_token:
                    break
                time.sleep(2)
                retry_count = 0
            except Exception as e:
                retry_count += 1
                print(f"[{username}] Error fetching posts (Attempt {retry_count}/{max_retries}): {str(e)}")
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
        except Exception as e:
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
        with file_lock:
            if os.path.exists(filename):
                try:
                    existing_df = pd.read_excel(filename, sheet_name=sheet_name)
                except Exception as e:
                    print(f"Existing file may be corrupted. Creating new file: {e}")
                    existing_df = pd.DataFrame({col: pd.Series(dtype=str) for col in df.columns})
            else:
                existing_df = pd.DataFrame({col: pd.Series(dtype=str) for col in df.columns})
    
            numeric_columns = ['like_count', 'comment_count', 'video_view_count', 'video_duration', 
                               'comments_count', 'like_count']
            for col in df.columns:
                if col in numeric_columns:
                    df[col] = df[col].fillna(0)
                    existing_df[col] = existing_df[col].fillna(0)
                else:
                    df[col] = df[col].fillna('')
                    existing_df[col] = existing_df[col].fillna('')
            for col in df.columns:
                if col in numeric_columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
                    existing_df[col] = pd.to_numeric(existing_df[col], errors='coerce').fillna(0).astype(int)
                else:
                    df[col] = df[col].astype(str).replace('nan', '')
                    existing_df[col] = existing_df[col].astype(str).replace('nan', '')
    
            if "post_code" in df.columns and "post_code" in existing_df.columns:
                existing_codes = set(existing_df["post_code"])
                df = df[~df["post_code"].isin(existing_codes)]
            elif "user_id" in df.columns and "user_id" in existing_df.columns:
                df['temp_key'] = df['post_code'] + '_' + df['user_id']
                existing_df['temp_key'] = existing_df['post_code'] + '_' + existing_df['user_id']
                existing_keys = set(existing_df['temp_key'])
                df = df[~df['temp_key'].isin(existing_keys)]
                df = df.drop('temp_key', axis=1)
    
            if not df.empty:
                final_df = pd.concat([existing_df, df], ignore_index=True)
                with pd.ExcelWriter(filename, engine='openpyxl', mode='w') as writer:
                    final_df.to_excel(writer, sheet_name=sheet_name, index=False)
                print(f"{len(df)} new rows added to {filename}.")
            else:
                print("No new data to add. All entries are duplicates.")
    except FileNotFoundError:
        with file_lock:
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        print(f"Created new file {filename} with {len(df)} rows.")
    except Exception as e:
        print(f"Error saving data to {filename}: {str(e)}")
        backup_filename = f"backup_{int(time.time())}_{filename}"
        with file_lock:
            df.to_excel(backup_filename, sheet_name=sheet_name, index=False)
        print(f"Backup file created: {backup_filename}")

def save_progress(post_code, pagination_token, comments):
    progress_data = {
        'post_code': post_code,
        'pagination_token': pagination_token,
        'last_comment_id': comments[-1]['id'] if comments else None,
        'comments_count': len(comments)
    }
    filename = 'comment_crawling_progress.xlsx'
    df = pd.DataFrame([progress_data])
    try:
        with file_lock:
            if os.path.exists(filename):
                existing_df = pd.read_excel(filename)
                existing_df = existing_df[existing_df['post_code'] != post_code]
                df = pd.concat([existing_df, df], ignore_index=True)
            df.to_excel(filename, index=False)
        print(f"Progress saved to {filename}")
    except Exception as e:
        print(f"Error saving progress: {str(e)}")

def load_progress(post_code):
    filename = 'comment_crawling_progress.xlsx'
    if not os.path.exists(filename):
        return None
    try:
        with file_lock:
            df = pd.read_excel(filename)
        progress = df[df['post_code'] == post_code].to_dict('records')
        return progress[0] if progress else None
    except Exception as e:
        print(f"Error loading progress: {str(e)}")
        return None

def save_crawling_status(username, post_code, status, message='', comment_pagination_token=None, comment_count=0):
    status_data = {
        'username': username,
        'post_code': post_code,
        'status': status,
        'timestamp': pd.Timestamp.now(),
        'message': message,
        'comment_pagination_token': comment_pagination_token,
        'collected_comments': comment_count
    }
    filename = 'crawling_status.xlsx'
    df = pd.DataFrame([status_data])
    try:
        with file_lock:
            if os.path.exists(filename):
                existing_df = pd.read_excel(filename)
                existing_df = existing_df[~((existing_df['username'] == username) & 
                                          (existing_df['post_code'] == post_code))]
                df = pd.concat([existing_df, df], ignore_index=True)
            df.to_excel(filename, index=False)
    except Exception as e:
        print(f"Error saving crawling status: {str(e)}")

def get_crawling_status(username):
    filename = 'crawling_status.xlsx'
    if not os.path.exists(filename):
        return []
    try:
        with file_lock:
            df = pd.read_excel(filename)
        user_status = df[df['username'] == username].sort_values('timestamp')
        return user_status.to_dict('records') if not user_status.empty else []
    except Exception as e:
        print(f"Error loading crawling status: {str(e)}")
        return []

def check_crawling_completion(username):
    """
    특정 사용자의 모든 게시물에 대한 크롤링 완료 상태를 체크합니다.
    """
    try:
        posts_file = f'instagram_posts_{username}.xlsx'
        comments_file = f'instagram_comments_{username}.xlsx'
        likers_file = f'instagram_likers_{username}.xlsx'
        
        # 게시물 데이터 확인
        if not os.path.exists(posts_file):
            print(f"[{username}] 게시물 파일이 없습니다.")
            return False
            
        posts_df = pd.read_excel(posts_file)
        if posts_df.empty:
            print(f"[{username}] 게시물 데이터가 없습니다.")
            return False
            
        total_posts = len(posts_df)
        print(f"[{username}] 총 {total_posts}개의 게시물이 있습니다.")
        
        # 댓글 데이터 확인
        comments_complete = True
        if os.path.exists(comments_file):
            comments_df = pd.read_excel(comments_file)
            post_comment_counts = comments_df.groupby('post_code').size()
            
            for post_code in posts_df['post_code']:
                comment_count = post_comment_counts.get(post_code, 0)
                expected_comments = min(MAX_COMMENTS_PER_POST, posts_df[posts_df['post_code'] == post_code]['comment_count'].iloc[0])
                
                if comment_count < expected_comments:
                    print(f"[{username}] 게시물 {post_code}의 댓글이 부족합니다. (현재: {comment_count}, 예상: {expected_comments})")
                    comments_complete = False
        else:
            print(f"[{username}] 댓글 파일이 없습니다.")
            comments_complete = False
            
        # 좋아요 데이터 확인
        likes_complete = True
        if os.path.exists(likers_file):
            likers_df = pd.read_excel(likers_file)
            post_liker_counts = likers_df.groupby('post_code').size()
            
            for _, post in posts_df.iterrows():
                post_code = post['post_code']
                liker_count = post_liker_counts.get(post_code, 0)
                expected_likes = post['like_count']
                
                if liker_count < expected_likes:
                    print(f"[{username}] 게시물 {post_code}의 좋아요가 부족합니다. (현재: {liker_count}, 예상: {expected_likes})")
                    likes_complete = False
        else:
            print(f"[{username}] 좋아요 파일이 없습니다.")
            likes_complete = False
            
        is_complete = comments_complete and likes_complete
        status_msg = "완료" if is_complete else "미완료"
        print(f"[{username}] 크롤링 상태: {status_msg}")
        print(f"- 댓글 상태: {'완료' if comments_complete else '미완료'}")
        print(f"- 좋아요 상태: {'완료' if likes_complete else '미완료'}")
        
        return is_complete
        
    except Exception as e:
        print(f"[{username}] 크롤링 상태 확인 중 오류 발생: {str(e)}")
        return False

def process_user_data(username, api_key):
    try:
        posts_file = f'instagram_posts_{username}.xlsx'
        if os.path.exists(posts_file):
            try:
                posts_df = pd.read_excel(posts_file)
                print(f"[{username}] Found existing posts file with {len(posts_df)} posts")
                posts_processed = posts_df.to_dict('records')
                new_posts = posts_processed
            except Exception as e:
                print(f"[{username}] Error reading posts file: {str(e)}")
                return
        else:
            posts_data = fetch_user_posts(api_key, username, max_posts=MAX_POSTS_PER_USER)
            if not posts_data or 'data' not in posts_data:
                save_crawling_status(username, '', 'error', 'Failed to fetch posts')
                return
            posts = posts_data['data'].get('items', [])
            print(f"[{username}] Retrieved {len(posts)} posts")
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
            print(f"[{username}] Saved {len(posts_processed)} posts to file")
            new_posts = posts_processed
    
        completed_posts = {status['post_code'] for status in get_crawling_status(username) if status['status'] == 'completed'}
        posts_to_process = [post for post in new_posts if post['post_code'] not in completed_posts]
    
        if not posts_to_process:
            print(f"[{username}] All posts have been processed.")
            return
    
        print(f"[{username}] Found {len(posts_to_process)} posts to process.")
        for post in posts_to_process:
            try:
                post_code = post['post_code']
                print(f"[{username}] Processing post: {post_code}")
                # 좋아요 데이터 수집
                likers_data = fetch_post_likers(api_key, post_code)
                if likers_data and 'data' in likers_data:
                    likers = likers_data['data'].get('items', [])
                    actual_like_count = len(likers)
                    try:
                        df = pd.read_excel(posts_file)
                        current_like_count = df.loc[df['post_code'] == post_code, 'like_count'].values[0]
                        print(f"[{username}] Current like count for {post_code}: {current_like_count}")
                        if current_like_count < actual_like_count:
                            df.loc[df['post_code'] == post_code, 'like_count'] = actual_like_count
                            df.to_excel(posts_file, index=False)
                            print(f"[{username}] Updated like count for {post_code}: {actual_like_count} (previous: {current_like_count})")
                        else:
                            print(f"[{username}] No update needed for {post_code}.")
                    except Exception as e:
                        print(f"[{username}] Error updating like count: {str(e)}")
    
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
                    print(f"[{username}] Saved {len(likers)} likes for post {post_code}")
                    save_crawling_status(username, post_code, 'likes_done')
    
                # 댓글 데이터 수집
                post_status = next((status for status in get_crawling_status(username) if status['post_code'] == post_code), None)
                comment_pagination_token = None
                collected_comments = 0
                if post_status:
                    if post_status['status'] == 'completed':
                        print(f"[{username}] Comments already completed for {post_code}")
                        continue
                    elif post_status['status'] == 'comments_in_progress':
                        token = post_status.get('comment_pagination_token')
                        comment_pagination_token = None if pd.isna(token) or token == 'nan' else token
                        collected_comments = int(post_status.get('collected_comments', 0))
                        if comment_pagination_token:
                            print(f"[{username}] Resuming comments from token: {comment_pagination_token}")
                        print(f"[{username}] Previously collected comments: {collected_comments}")
    
                while True:
                    try:
                        remaining_comments = MAX_COMMENTS_PER_POST - collected_comments
                        if remaining_comments <= 0:
                            print(f"[{username}] Reached maximum comment limit for {post_code}")
                            save_crawling_status(username, post_code, 'completed', comment_count=collected_comments)
                            break
                        comments_data = fetch_post_comments(api_key, post_code, pagination_token=comment_pagination_token, collected_count=collected_comments)
                        if not comments_data or 'data' not in comments_data:
                            save_crawling_status(username, post_code, 'completed', comment_count=collected_comments)
                            break
                        comments = comments_data['data'].get('items', [])
                        if not comments:
                            save_crawling_status(username, post_code, 'completed', comment_count=collected_comments)
                            break
                        if collected_comments + len(comments) > MAX_COMMENTS_PER_POST:
                            comments = comments[:MAX_COMMENTS_PER_POST - collected_comments]
                        print(f"[{username}] Processing {len(comments)} comments for {post_code}. Total will be: {collected_comments + len(comments)}")
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
                            save_data_to_excel(comments_processed, f'instagram_comments_{username}.xlsx', 'Comments')
                            collected_comments += len(comments_processed)
                            print(f"[{username}] Saved {len(comments_processed)} new comments. Total: {collected_comments}")
                        if collected_comments >= MAX_COMMENTS_PER_POST:
                            print(f"[{username}] Reached maximum comment limit for {post_code}")
                            save_crawling_status(username, post_code, 'completed', comment_count=collected_comments)
                            break
                        comment_pagination_token = comments_data.get('pagination_token')
                        if not comment_pagination_token:
                            print(f"[{username}] No more comments for {post_code}")
                            save_crawling_status(username, post_code, 'completed', comment_count=collected_comments)
                            break
                        else:
                            save_crawling_status(username, post_code, 'comments_in_progress', comment_pagination_token=comment_pagination_token, comment_count=collected_comments)
                            print(f"[{username}] Saved progress with token: {comment_pagination_token}")
                        time.sleep(1)
                    except KeyboardInterrupt:
                        print(f"[{username}] Crawling interrupted by user for post {post_code}")
                        save_crawling_status(username, post_code, 'comments_in_progress', comment_pagination_token=comment_pagination_token, comment_count=collected_comments)
                        raise
                    except Exception as e:
                        print(f"[{username}] Error in comments loop for {post_code}: {str(e)}")
                        save_crawling_status(username, post_code, 'error', message=str(e), comment_pagination_token=comment_pagination_token, comment_count=collected_comments)
                        break
                time.sleep(1)
            except KeyboardInterrupt:
                raise
            except Exception as e:
                print(f"[{username}] Error processing post {post.get('post_code')}: {str(e)}")
                continue
    except KeyboardInterrupt:
        print(f"[{username}] Crawling interrupted by user. Progress has been saved.")
        raise
    except Exception as e:
        print(f"[{username}] Error in process_user_data: {str(e)}")

def process_user_data_threadsafe(username, api_key):
    print(f"Starting processing for user: {username}")
    process_user_data(username, api_key)
    print(f"Completed processing for user: {username}")

def check_crawling_status_for_users():
    """
    instagram_users.xlsx 파일에 있는 유저들의 크롤링 상태만 체크합니다.
    """
    try:
        input_file = "instagram_users.xlsx"
        if not os.path.exists(input_file):
            print("instagram_users.xlsx 파일이 없습니다.")
            return False
            
        users_df = pd.read_excel(input_file)
        if users_df.empty:
            print("instagram_users.xlsx 파일에 유저 정보가 없습니다.")
            return False
            
        usernames = users_df['username'].tolist()
        print(f"\n총 {len(usernames)}명의 유저에 대해 크롤링 상태를 확인합니다.\n")
        
        all_complete = True
        incomplete_users = []
        
        for username in usernames:
            print(f"\n=== {username} 크롤링 상태 확인 ===")
            if not check_crawling_completion(username):
                all_complete = False
                incomplete_users.append(username)
            
        if incomplete_users:
            print(f"\n미완료된 유저 목록: {', '.join(incomplete_users)}")
        print("\n모든 유저의 크롤링 상태 확인이 완료되었습니다.")
        
        return all_complete
        
    except Exception as e:
        print(f"크롤링 상태 확인 중 오류 발생: {str(e)}")
        return False

def main():
    api_key = "fb1be9caf7mshc1fea79903f370fp1c0b11jsn97070d82933d"
    input_file = "instagram_users.xlsx"
    try:
        users_df = pd.read_excel(input_file)
        usernames = users_df['username'].tolist()
        
        # 사용자에게 스레드 수 입력받기
        while True:
            try:
                max_workers = int(input("사용할 스레드 수를 입력하세요 (1-30): "))
                if 1 <= max_workers <= 30:
                    break
                else:
                    print("1에서 30 사이의 숫자를 입력해주세요.")
            except ValueError:
                print("올바른 숫자를 입력해주세요.")
        
        print(f"{max_workers}개의 스레드로 크롤링을 시작합니다...")
        
        # 크롤링 상태 체크
        all_complete = check_crawling_status_for_users()
        
        if all_complete:
            print("\n모든 유저의 크롤링이 완료되었습니다. 프로그램을 종료합니다.")
            return
            
        print("\n미완료된 유저들에 대해 크롤링을 시작합니다...\n")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(process_user_data_threadsafe, username, api_key) 
                      for username in usernames]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Error in thread: {str(e)}")
                    
    except KeyboardInterrupt:
        print("Program interrupted by user")
    except Exception as e:
        print(f"Error processing users: {str(e)}")
    finally:
        print("Program finished")

if __name__ == "__main__":
    main()