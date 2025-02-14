import pandas as pd
import webbrowser

profiles = [
    'angy__unni',
    'haeinny83',
    'songjenny___',
    'gs___park',
    'ellie_youngok',
    'usangmoo',
    'smitruti_food',
    'from.dew_',
    'from_da_hye',
    'rinjoo__',
    'bae_jita',
    '95.404',
]

def open_instagram_links():
    # username으로 인스타그램 링크 생성
    for username in profiles:
        instagram_url = f'https://www.instagram.com/{username}/'

        # 브라우저에서 링크 열기
        webbrowser.open(instagram_url)

open_instagram_links()
