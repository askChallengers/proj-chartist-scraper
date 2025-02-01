from urllib.parse import urljoin
import pandas as pd
import re
import requests
import xmltodict
import pytz
from typing import List
from datetime import datetime
import re
import time
import random
from urllib.parse import urljoin
import inspect
import googleapiclient.discovery
import googleapiclient.errors

from src.config.helper import log_method_call
from src.connection.gsheets import GSheetsConn
from src.config.env import GOOGLE_SHEET_URL, EXECUTE_ENV, GCP_CREDENTIAL_API_KEY

# KST (Korea Standard Time) 시간대를 설정
kst = pytz.timezone('Asia/Seoul')
cur = datetime.now(kst)

class Youtube():
    base_url = 'https://www.youtube.com'
    @log_method_call
    def __init__(self):
        super().__init__()

        # 공식 가이드: https://developers.google.com/youtube/v3/docs/videos?hl=ko
        # API 서비스 정보 설정
        api_service_name = "youtube"
        api_version = "v3"
        api_key = GCP_CREDENTIAL_API_KEY  # 여기에 발급받은 API 키를 입력하세요

        # API 클라이언트 생성
        self.client = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)

    def get_channel_info_by_custom_url(self, custom_url: str) -> dict:
        if not custom_url.startswith('@'):
            raise Exception('CUSTOM_URL은 @로 시작해야 합니다.')
        
        # API 요청 설정
        request = self.client.channels().list(
            part="id",
            forHandle=custom_url
        )
        response = request.execute()
        item = response['items'][0]
        return item
    
    def get_channel_info_by_channel_id(self, id_list: List[str]) -> dict:
        # API 요청 설정
        id_list_str = ','.join(id_list)
        request = self.client.channels().list(
            part="id,snippet,contentDetails,statistics",
            id=id_list_str
        )
        response = request.execute()
        return response['items']
    
    def get_video_info_by_video_id(self, id_list: List[str]) -> pd.DataFrame:
        # API 요청 설정
        id_list_str = ','.join(id_list)
        request = self.client.videos().list(
            part="snippet,contentDetails,statistics",
            id=id_list_str
        )
        response = request.execute()
        items = response['items']
        result = []
        for item in items:
            result += [{
                'mv_id' : item['id'],
                'mv_channel_id' : item['snippet']['channelId'],
                'mv_title' : item['snippet']['title'],
                'view_count' : int(item['statistics']['viewCount']),
            }]
        return pd.DataFrame(result)

    @log_method_call
    def get_search_video_result(self, keyword:str, channelId:str=None, maxResults:int=5) -> List[dict]:
        request = self.client.search().list(
            part="snippet",
            channelId=channelId,
            maxResults=maxResults,
            order="relevance",
            q=keyword,
            type='video',
        )
        response = request.execute()
        return response['items']