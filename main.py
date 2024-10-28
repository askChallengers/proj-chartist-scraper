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
meta_by_youtube = meta_by_youtube.merge(channel_info[['channel', 'img_url']], on='channel', how='left')

# meta_by_3rd_party = youtube_scraper.crawl_content_info_by_3rd_party(meta_by_youtube['mv_identifier'].unique())
# total_youtube_info = meta_by_youtube.merge(meta_by_3rd_party, on='mv_identifier', how='left')

total_info = target_info_by_vibe.merge(
    meta_by_youtube,
    on='searchKeyword',
    how='left',
)

total_info['reg_date'] = pd.to_datetime(today)
today_str = today.strftime('%Y-%m-%d')
bq_conn.upsert(df=total_info, table_id='daily_report', data_set='chartist', target_dict={'reg_date': today_str})