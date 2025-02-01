import pytz
import warnings
from datetime import datetime
import pandas as pd
from src.connection.bigquery import BigQueryConn
from src.scrapers.scraper import BaseScraper

warnings.filterwarnings('ignore')

# KST (Korea Standard Time) 시간대를 설정
today = datetime.now(pytz.timezone('Asia/Seoul')).date()
bq_conn = BigQueryConn()
scraper = BaseScraper()

# 수기입력 데이터 관리하는 구글시트 업데이트
scraper.update_channe_id()
scraper.update_img_url()

total_info = scraper.vibe_client.get_target_info_by_vibe(
    ranking=100,
    except_albums=scraper.except_albums, 
    except_artists=scraper.except_artists
)
temp_cols = ['is_new_artist','is_new_mv']
total_info.loc[:, temp_cols] = False

total_info = scraper.fetch_meta_info(df=total_info)
total_info = scraper.fetch_search_mv_info(df=total_info)

total_info['is_official_channel'] = total_info['mv_channel_id'].apply(
    lambda x: True if x in scraper.official_channels['channel_id'].unique() else False
)

color_cnt = 3
color_cols = [f'color_{i+1}' for i in range(color_cnt)]
total_info = scraper.fetch_color_info(df=total_info, color_cnt=color_cnt)

scraper.slack_alert(df=total_info)
total_info = total_info.drop(columns=temp_cols)

total_info['reg_date'] = pd.to_datetime(today)
total_info['month'] = total_info['reg_date'].dt.month  # 월 추출
total_info['week_of_month'] = (total_info['reg_date'].dt.day - 1) // 7 + 1  # 몇째 주 계산
total_info['week_of_month'] = total_info['week_of_month'].apply(lambda x: '1st' if x == 1 else '2nd' if x == 2 else str(x)+'th')

today_str = today.strftime('%Y-%m-%d')

total_info = total_info[[
    # vibe 기반
    'gender', 'artistId', 'artistName', 'trackTitle', 'albumId', 'albumTitle', 'vibe_rank', 
    # 구글시트 기반
    'artist_custom_url', 'artist_channel_id', 'artist_img_url', 
    # 유튜브 기반
    'searchKeyword', 'mv_channel_id', 'mv_id', 'mv_title', 'view_count', 
    # 기타
    'is_official_channel', 
    # 날짜 관련
    'reg_date', 'month', 'week_of_month'
] + color_cols]
bq_conn.upsert(df=total_info, table_id='daily_report', data_set='chartist', target_dict={'reg_date': today_str})