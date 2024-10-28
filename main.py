import pytz
import warnings
from datetime import datetime
import pandas as pd
from src.scrapers import VibeScraper, YoutubeScraper
from src.connection.bigquery import BigQueryConn
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

total_info['reg_date'] = pd.to_datetime(today)
today_str = today.strftime('%Y-%m-%d')

total_info = total_info[[
    'gender', 'artistId', 'artistName', 'trackTitle', 'albumId', 'albumTitle', 'vibe_rank', 
    'searchKeyword', 
    'channel', 'img_url',
    'mv_channel', 'mv_identifier', 'mv_title', 'mv_link', 'view_count', 'is_official_channel',
    'reg_date'
]]
bq_conn.upsert(df=total_info, table_id='daily_report', data_set='chartist', target_dict={'reg_date': today_str})