from abc import ABC
import pandas as pd
import re
import requests
import xmltodict
import pytz
from datetime import datetime

from src.config.helper import log_method_call
from src.connection.gsheets import GSheetsConn
from src.connection.bigquery import BigQueryConn
from src.connection.cloud_storage import GCSConn
from src.config.env import GOOGLE_SHEET_URL
from src.connection.slack import SlackClient
from src.color_extractor import get_dominant_color_by_url
from .vibe_api import Vibe
from .youtube_api import Youtube

# KST (Korea Standard Time) 시간대를 설정
kst = pytz.timezone('Asia/Seoul')
cur = datetime.now(kst)

def requests_get_xml(url) -> dict:
    response = requests.get(url)
    xml_data = response.content
    return xmltodict.parse(xml_data)

class BaseScraper(ABC):
    # 클래스 변수
    gs_cleint = GSheetsConn(url=GOOGLE_SHEET_URL)
    bq_cleint = BigQueryConn()
    gcs_client = GCSConn(bucket='team-ask-storage')

    vibe_client = Vibe()
    youtube_client = Youtube()

    except_albums = gs_cleint.get_df_from_google_sheets(sheet='except_albums')
    except_artists = gs_cleint.get_df_from_google_sheets(sheet='except_artists')
    official_channels = gs_cleint.get_df_from_google_sheets(sheet='official_channels')
    
    @log_method_call
    def __init__(self):
        self.init_gss_data()

    def init_gss_data(self):
        self.official_channels.replace('', None, inplace=True)
        self.official_channels['artistId'] = pd.to_numeric(self.official_channels['artistId'])

        self.except_albums['artistId'] = pd.to_numeric(self.except_albums['artistId']) 
        self.except_albums['albumId'] = pd.to_numeric(self.except_albums['albumId'])
        self.except_artists['artistId'] = pd.to_numeric(self.except_artists['artistId'])

    def _health_check_img_url(self, img_url: str):
        response = requests.get(img_url)
        if response.status_code == 200:
            return True
        return False
    
    @log_method_call
    def update_channe_id(self):
        update_targets = []
        for idx in self.official_channels.index:
            _custom_url = self.official_channels.at[idx, 'custom_url']
            _channel_id = self.official_channels.at[idx, 'channel_id']
            if _channel_id is not None:
                continue
            update_targets += [_custom_url]

        if len(update_targets) == 0:
            print('업데이트할 대상이 없습니다.')
            return
        
        new_dict = {}
        for _t in update_targets:
            item = self.youtube_client.get_channel_info_by_custom_url(_t)
            new_dict[_t] = item['id']

        # 기존 시트 형태의 dataframe에 맞춰 넣기
        for idx in self.official_channels.index:
            tmp_channel = self.official_channels.at[idx, 'custom_url']
            if tmp_channel in new_dict.keys():
                self.official_channels.at[idx, 'channel_id'] = new_dict[tmp_channel]
                self.official_channels.at[idx, 'update_dt'] = cur.strftime('%Y-%m-%d %H:%M:%S')

        # 구글 시트 업데이트
        sheet = GSheetsConn(url=GOOGLE_SHEET_URL).get_worksheet(sheet='official_channels')
        self.gs_cleint.update_google_sheet_column(self.official_channels, 'channel_id', sheet)
        self.gs_cleint.update_google_sheet_column(self.official_channels, 'update_dt', sheet)


    @log_method_call
    def update_img_url(self):
        # 업데이트 대상 추리기
        update_targets = []
        for idx in self.official_channels.index:
            _img_url = self.official_channels.at[idx, 'img_url']
            _channel_id = self.official_channels.at[idx, 'channel_id']
            if (_img_url is not None) and self._health_check_img_url(_img_url):
                continue
            update_targets += [_channel_id]

        if len(update_targets) == 0:
            print('업데이트할 대상이 없습니다.')
            return
        
        # 업데이트 내용 가져오기
        items = self.youtube_client.get_channel_info_by_channel_id(update_targets)
        new_dict = {}
        for item in items:
            new_dict[item['id']] = item['snippet']['thumbnails']['default']['url']

        # 기존 시트 형태의 dataframe에 맞춰 넣기
        for idx in self.official_channels.index:
            tmp_channel = self.official_channels.at[idx, 'channel_id']
            if tmp_channel in new_dict.keys():
                self.official_channels.at[idx, 'img_url'] = new_dict[tmp_channel]
                self.official_channels.at[idx, 'update_dt'] = cur.strftime('%Y-%m-%d %H:%M:%S')

        # 구글 시트 업데이트
        sheet = GSheetsConn(url=GOOGLE_SHEET_URL).get_worksheet(sheet='official_channels')
        self.gs_cleint.update_google_sheet_column(self.official_channels, 'img_url', sheet)
        self.gs_cleint.update_google_sheet_column(self.official_channels, 'update_dt', sheet)

    @log_method_call
    def slack_alert(self, df: pd.DataFrame):
        new_artists = df.loc[df['is_new_artist'] == True]
        if new_artists.shape[0] != 0:
            title = "🫠🫠[PROJ-CHARTIST-SCRAPER: 신규 아티스트 이슈]🫠🫠"
            contents = ''
            for idx in new_artists.index:
                _id = new_artists.at[idx, 'artistId']
                _nm = new_artists.at[idx, 'artistName']
                contents += f'✅ `{_id}`: {_nm}\n'
            SlackClient().chat_postMessage(title, contents)
        
        new_mvs = df.loc[df['is_new_mv'] == True]
        if new_mvs.shape[0] != 0:
            title = "📺📺[PROJ-CHARTIST-SCRAPER: 신규 뮤직비디오 이슈]📺📺"
            contents = ''
            for idx in new_mvs.index:
                _keyword = new_mvs.at[idx, 'searchKeyword']
                _mv_id = new_mvs.at[idx, 'mv_id']
                _mv_nm = new_mvs.at[idx, 'mv_title']
                _url = f"https://www.youtube.com/watch?v={_mv_id}"
                contents += f'✅ `{_keyword}`: <{_url}|{_mv_nm}>\n'
            SlackClient().chat_postMessage(title, contents)

        unofficial_channel = df.loc[df['is_official_channel'] == False]
        if unofficial_channel.shape[0] != 0:
            title = "💈💈[PROJ-CHARTIST-SCRAPER: 신규 채널 이슈]💈💈"
            contents = ''
            for idx in unofficial_channel.index:
                _nm = unofficial_channel.at[idx, 'artistName']
                _mv_id = unofficial_channel.at[idx, 'mv_id']
                _mv_nm = unofficial_channel.at[idx, 'mv_title']
                _url = f"https://www.youtube.com/watch?v={_mv_id}"
                contents += f'✅ `{_nm}`: <{_url}|{_mv_nm}>\n'
            SlackClient().chat_postMessage(title, contents)

    @log_method_call
    def fetch_meta_info(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        meta_info = self.official_channels[~self.official_channels['artistId'].isna()].rename(
            columns={
                'channel_id': 'artist_channel_id',
                'custom_url': 'artist_custom_url',
                'img_url': 'artist_img_url'
            }
        ).drop(columns=['type', 'artistName', 'update_dt'])

        result = result.merge(
            meta_info,
            on='artistId',
            how='left'
        )
        result.loc[result['artist_img_url'].isna(), 'is_new_artist'] = True
        return result

    @log_method_call
    def fetch_search_mv_info(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result['searchKeyword'] = result.apply(lambda x: f"{x['artistName']} {x['trackTitle']} official MV", axis=1)

        mv_id_df = self.bq_cleint.query('''
        SELECT DISTINCT
            searchKeyword, mv_id
        FROM `team-ask-infra.chartist.daily_report`
        WHERE 1=1
        ''')
        result = result.merge(mv_id_df, on='searchKeyword', how='left')

        searchKeyword_list = result.loc[result['mv_id'].isna(), 'searchKeyword'].to_list()


        for _q in searchKeyword_list:
            item = self.youtube_client.get_search_video_result(keyword=_q)[0]
            mv_id = item['id']['videoId']
            cond = result['searchKeyword'] == _q
            result.loc[cond, 'mv_id'] = mv_id
            result.loc[cond, 'is_new_mv'] = True

        mv_info_df = self.youtube_client.get_video_info_by_video_id(result['mv_id'].unique())
        result = result.merge(
            mv_info_df, on='mv_id', how='left',
        )
        return result
    
    @log_method_call
    def fetch_color_info(self, df: pd.DataFrame, color_cnt: int) -> pd.DataFrame:
        result = df.copy()
        color_df = []
        for idx in result.loc[~result['artist_img_url'].isna()].index:
            url = result.at[idx, 'artist_img_url']
            dominant_colors = get_dominant_color_by_url(url=url, cnt=color_cnt)
            color_df += [dominant_colors]
        color_df = pd.DataFrame(color_df).rename(columns={'img_url': 'artist_img_url'})
        result = result.merge(color_df, on='artist_img_url', how='left')
        return result