import pytz
import warnings
from datetime import datetime
import pandas as pd
from src.scrapers.vibe_scraper import VibeScraper
from src.scrapers.youtube_scraper import YoutubeScraper
from src.connection.bigquery import BigQueryConn
from src.connection.slack import SlackClient
from src.color_extractor import get_dominant_color_by_url
warnings.filterwarnings('ignore')

# KST (Korea Standard Time) 시간대를 설정
today = datetime.now(pytz.timezone('Asia/Seoul')).date()
bq_conn = BigQueryConn()
vibe_scraper = VibeScraper()
youtube_scraper = YoutubeScraper(is_headless=True)

target_info_by_vibe = vibe_scraper.get_target_info_by_vibe(100)
target_info_by_vibe['searchKeyword'] = target_info_by_vibe.apply(lambda x: f"{x['artistName']} {x['trackTitle']} official MV", axis=1)
channel_info = youtube_scraper.update_channel_info_sheet()

meta_by_youtube = youtube_scraper.crawl_youtube_search(target_info_by_vibe['searchKeyword'].unique())

total_info = target_info_by_vibe.merge(
    meta_by_youtube,
    on='searchKeyword',
    how='left',
).merge(
    channel_info[['artistId', 'channel', 'img_url']], on='artistId', how='left'
)

color_cnt = 3
color_df = []
color_cols = [f'color_{i+1}' for i in range(color_cnt)]
for idx in total_info.loc[~total_info['img_url'].isna()].index:
    url = total_info.at[idx, 'img_url']
    dominant_colors = get_dominant_color_by_url(url=url, cnt=color_cnt)
    color_df += [dominant_colors]
color_df = pd.DataFrame(color_df)
total_info = total_info.merge(color_df, on='img_url', how='left')

except_artist = total_info.loc[total_info['img_url'].isna(), ['artistId', 'artistName']].drop_duplicates()

if not except_artist.empty:
    title = "🚨[PROJ-CHARTIST-SCRAPER: 예외 아티스트 이슈]🚨"
    contents = ''
    for idx in except_artist.index:
        _id = except_artist.at[idx, 'artistId']
        _nm = except_artist.at[idx, 'artistName']
        contents += f'*{_id}*: {_nm}\n'
    SlackClient().chat_postMessage(title, contents)

total_info['reg_date'] = pd.to_datetime(today)
total_info['month'] = total_info['reg_date'].dt.month  # 월 추출
total_info['week_of_month'] = (total_info['reg_date'].dt.day - 1) // 7 + 1  # 몇째 주 계산
total_info['week_of_month'] = total_info['week_of_month'].apply(lambda x: '1st' if x == 1 else '2nd' if x == 2 else str(x)+'th')

today_str = today.strftime('%Y-%m-%d')

total_info = total_info[[
    'gender', 'artistId', 'artistName', 'trackTitle', 'albumId', 'albumTitle', 'vibe_rank', 
    'searchKeyword', 
    'channel', 'img_url',
    'mv_channel', 'mv_identifier', 'mv_title', 'mv_link', 'view_count', 'is_official_channel',
    'reg_date', 'month', 'week_of_month'
] + color_cols]
bq_conn.upsert(df=total_info, table_id='daily_report', data_set='chartist', target_dict={'reg_date': today_str})