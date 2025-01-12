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
from src.config.env import GOOGLE_SHEET_URL

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
    except_albums = gs_cleint.get_df_from_google_sheets(sheet='except_albums')
    except_artists = gs_cleint.get_df_from_google_sheets(sheet='except_artists')
    official_channels = gs_cleint.get_df_from_google_sheets(sheet='official_channels')
    
    @log_method_call
    def __init__(self):
        self.official_channels.replace('', None, inplace=True)
        self.official_channels['artistId'] = pd.to_numeric(self.official_channels['artistId'])

        self.except_albums['artistId'] = pd.to_numeric(self.except_albums['artistId']) 
        self.except_albums['albumId'] = pd.to_numeric(self.except_albums['albumId'])

        self.except_artists['artistId'] = pd.to_numeric(self.except_artists['artistId'])
    