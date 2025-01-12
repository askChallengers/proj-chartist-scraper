from abc import ABC
from urllib.parse import urljoin
import pandas as pd
import re
import requests

import pytz
from datetime import datetime

import time
from urllib.parse import urljoin

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from src.config.helper import log_method_call
from src.connection.gsheets import GSheetsConn
from src.connection.bigquery import BigQueryConn
from src.config.env import GOOGLE_SHEET_URL, EXECUTE_ENV

# KST (Korea Standard Time) 시간대를 설정
kst = pytz.timezone('Asia/Seoul')
cur = datetime.now(kst)

def requests_get_xml(url) -> dict:
    response = requests.get(url)
    xml_data = response.content
    return xmltodict.parse(xml_data)

class Scraper(ABC):
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
    
class VibeScraper(Scraper):
    base_url = 'https://apis.naver.com'

    @log_method_call
    def __init__(self):
        super().__init__()
    
    def get_artist_info(self, artistId: str) -> dict:
        end_point = 'vibeWeb/musicapiweb/vibe/v1/artist/<artistId>/info.json'.replace('<artistId>', str(artistId))
        url = urljoin(self.base_url, end_point)
        response = requests.get(url)
        dict_data = response.json()['response']['result']['artistEnd']
        keys_to_get = ['artistId', 'gender', 'isGroup', 'managementName', 'biography', 'genreNames']
        filtered_data = {key: dict_data[key] for key in keys_to_get if key in dict_data}
        return filtered_data

    @log_method_call
    def get_top100_chart(self) -> pd.DataFrame:
        end_point = 'vibeWeb/musicapiweb/vibe/v1/chart/track/genres/DS101?start=1&display=100'
        url = urljoin(self.base_url, end_point)
        dict_data = requests_get_xml(url)
        track_list = dict_data['response']['result']['chart']['items']['tracks']['track']
        result = pd.DataFrame(columns=['vibe_rank', 'trackTitle', 'artistName', 'artistId'])
        for _r, _track in enumerate(track_list):
            trackTitle = _track['trackTitle']
            if isinstance(_track['artists']['artist'], list):
                artistName_list = list(map(lambda x: x['artistName'], _track['artists']['artist']))
                artistId_list = list(map(lambda x: x['artistId'], _track['artists']['artist']))
                artistName, artistId = artistName_list[0], artistId_list[0]
            else:
                artistName = _track['artists']['artist']['artistName']
                artistId = _track['artists']['artist']['artistId']
            row = pd.DataFrame([{'vibe_rank': _r+1, 'trackTitle': trackTitle, 'artistName': artistName, 'artistId': artistId}])
            result = pd.concat([result, row], ignore_index=True)
        result['artistId'] = result['artistId'].astype(int)
        result['vibe_rank'] = result['vibe_rank'].astype(int)
        return result

    def get_latest_album_info_by_artistId(self, artistId: int, block_albumIds: list) -> pd.DataFrame:
        end_point = 'vibeWeb/musicapiweb/v3/musician/artist/<artistId>/albums?start=1&display=10&type=ALL&sort=newRelease'.replace('<artistId>', str(artistId))
        url = urljoin(self.base_url, end_point)
        dict_data = requests_get_xml(url)
        album_list = dict_data['response']['result']['albums']['album']
        album_list = album_list if isinstance(album_list, list) else [album_list]
        for _album in album_list:
            # 제외: 협업 OR 자체블락
            specific_info = self.get_specific_album_info(int(_album['albumId']))
            if specific_info['albumGenres'] == 'J-팝' or specific_info['artistTotalCount'] > 1 or int(_album['albumId']) in block_albumIds:
                continue
            track_list_df = self.get_tracks_info_by_albumId(_album['albumId'])
            latest_album = pd.DataFrame([_album])[['albumId', 'albumTitle', 'releaseDate', 'imageUrl']]
            result = latest_album.merge(track_list_df, on='albumId', how='left')
            break
        result['artistId'] = artistId
        result['albumId'] = pd.to_numeric(result['albumId'])
        return result

    def get_specific_album_info(self, albumId: int) -> dict:
        end_point = 'vibeWeb/musicapiweb/album/<albumId>?includeDesc=true&includeIntro=true'.replace('<albumId>', str(albumId))
        url = urljoin(self.base_url, end_point)
        dict_data = requests_get_xml(url)
        album_info = dict_data['response']['result']['album']
        album_info['artistTotalCount'] = int(album_info['artistTotalCount'])
        return album_info
    
    def get_tracks_info_by_albumId(self, albumId: int) -> pd.DataFrame:
        end_point = 'vibeWeb/musicapiweb/album/<albumId>/tracks?start=1&display=1000'.replace('<albumId>', str(albumId))
        url = urljoin(self.base_url, end_point)
        dict_data = requests_get_xml(url)
        
        trackTotalCount = int(dict_data['response']['result']['trackTotalCount'])
        if trackTotalCount == 1:
            result = pd.DataFrame([dict_data['response']['result']['tracks']['track']])
        else:
            result = pd.DataFrame(dict_data['response']['result']['tracks']['track'])
        
        result[['trackId', 'likeCount']] = result[['trackId', 'likeCount']].astype(int)
        result[['represent', 'isOversea', 'isTopPopular']] = result[['represent', 'isOversea', 'isTopPopular']].replace({
            'true': True, 
            'false': False
        })
        result['albumId'] = albumId
        return result[
            ['trackId', 'trackTitle', 'represent', 'albumId', 'isOversea', 'likeCount', 'score', 'isTopPopular']
        ]
    
    @log_method_call
    def get_target_info_by_vibe(self, ranking:int=100):
        # top100 차트에서 가수 정보만 추출한다.
        chart_df = self.get_top100_chart()
        artist_info = chart_df.loc[
            chart_df['vibe_rank'].isin(range(ranking+1)) &\
            ~chart_df['artistId'].isin(self.except_artists['artistId']),
            ['vibe_rank', 'artistId', 'artistName']
        ].sort_values(by='vibe_rank').drop_duplicates(keep='first')

        # 각 타겟 가수별 제외 대상 앨범 외의 최신 앨범 정보를 가져온다.
        latest_album_info_by_artistId = []
        for _artistId in artist_info['artistId'].unique():
            specific_artist_info = self.get_artist_info(_artistId)
            if specific_artist_info['isGroup'] == False:
                continue
            if specific_artist_info['gender'] not in ('남성', '여성'):
                continue

            tmp_block_album_list =  Scraper.except_albums[lambda x: x['artistId'] == _artistId]['albumId'].to_list()
            latest_album_info = self.get_latest_album_info_by_artistId(int(_artistId), tmp_block_album_list)
            tmp = latest_album_info.merge(pd.DataFrame([specific_artist_info]), on='artistId', how='left')
            latest_album_info_by_artistId += [tmp]
        latest_album_info_by_artistId = pd.concat(latest_album_info_by_artistId).reset_index(drop=True)

        # 타겟 가수별 최신 앨범 정보를 가수 정보에 붙인다.
        total_info = artist_info.merge(latest_album_info_by_artistId, on='artistId', how='left')

        # 가수:앨범:노래=1:1:1로 하기 위해, 타이틀곡에 대해서만 가져오되, 멀티 타이틀인 경우, 그 중 likeCount 높은 걸 가져온다.
        total_info = total_info.loc[total_info['represent'] == True]\
            .sort_values(by=['gender', 'vibe_rank'], ascending=False)\
            .drop_duplicates(subset=['artistId'], keep='first')[
            ['gender', 'artistId', 'artistName', 'trackTitle', 'albumId', 'albumTitle', 'vibe_rank']
        ].reset_index(drop=True)

        return total_info

class YoutubeScraper(Scraper):
    base_url = 'https://www.youtube.com'
    @log_method_call
    def __init__(self, is_headless: bool):
        super().__init__()
        self.chrome_options = webdriver.ChromeOptions()
        # 한국어 언어 설정
        self.chrome_options.add_argument("--lang=ko-KR")
        self.chrome_options.add_argument("Accept-Language=ko-KR")
        if EXECUTE_ENV == 'LOCAL':
            self.service = Service(ChromeDriverManager().install())
        else:
            self.service = None
            self.chrome_options.add_argument('--disable-dev-shm-usage') # 공유 메모리 사용하지 않도록 하는 옵션

        if is_headless:
            self.chrome_options.add_argument("--no-sandbox") #샌드박스 모드 해제(보안 문제있을 수 있음)
            self.chrome_options.add_argument('window-size=1920x1080')
            self.chrome_options.add_argument("disable-gpu")
            self.chrome_options.add_argument('headless')
        else:
            self.chrome_options = None
    
    def _parse_content_count_info(self, mv_link: str, driver: webdriver.Chrome) -> dict:
        try:
            driver.get(mv_link)
            xpath_value = '//*[@id="watch7-content"]/meta[11]'
            WebDriverWait(driver, 30).until(EC.presence_of_all_elements_located((By.XPATH, xpath_value)))
            str_view_count = driver.find_element(by=By.XPATH, value=xpath_value).get_attribute('content')
            view_count = int(re.sub(r'[^0-9]', '', str_view_count))
            return {'view_count': view_count}
        except TimeoutException:
            print('- TIMEOUT: GET THE FAKE KEYWORD')
            driver.get('https://www.youtube.com/results?search_query=FAKE_KEYWORD')
            time.sleep(3)
        except Exception as e:
            raise e

    def _parse_channel_url(self, channel_href: str, driver: webdriver.Chrome):
        driver.get(channel_href)
        channel_section = '//*[@id="page-header"]/yt-page-header-renderer/yt-page-header-view-model/div/div[1]/div/yt-content-metadata-view-model/div[1]/span'
        WebDriverWait(driver, 30).until(EC.presence_of_all_elements_located((By.XPATH, channel_section)))
        channel = driver.find_element(by=By.XPATH, value=channel_section).text
        return channel
    
    @log_method_call
    def _parse_content_info_by_youtube(self, keyword: str, driver: webdriver.Chrome) -> dict:
        end_point = f'results?search_query={keyword}'
        url = urljoin(self.base_url, end_point)
        try:
            driver.get(url)
            elements = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.XPATH, '//*[@id="contents"]/ytd-video-renderer')))
            # 검색 후 최상단에 있는 것만 파싱해서 가져온다.
            elem = elements[0]
            specific_title_elem = elem.find_element(by=By.XPATH, value='.//*[@id="video-title"]')
            mv_title = specific_title_elem.get_attribute("title")
            mv_identifier = specific_title_elem.get_attribute("href")
            mv_identifier = mv_identifier.split('/watch?')[1].split('v=')[1].split('&')[0]
            mv_link = f'https://www.youtube.com/watch?v={mv_identifier}'
            channel = elem.find_element(by=By.XPATH, value='.//*[@id="channel-thumbnail"]').get_attribute("href")
            if not channel.startswith('@'):
                channel = self._parse_channel_url(channel_href=channel, driver=driver)
            channel = channel.replace('https://www.youtube.com/', '')
            mv_count_info = self._parse_content_count_info(mv_link=mv_link, driver=driver)
            return {
                'searchKeyword': keyword,
                'mv_channel': channel,
                'mv_identifier': mv_identifier,
                'mv_title': mv_title,
                'mv_link':mv_link,
                'view_count': mv_count_info['view_count'],
                # 'comment_count': mv_count_info['comment_count'],
            }
        except TimeoutException:
            print('- TIMEOUT: GET THE FAKE KEYWORD')
            driver.get('https://www.youtube.com/results?search_query=FAKE_KEYWORD')
            time.sleep(3)
        except Exception as e:
            raise e
    
    @log_method_call
    def _parse_content_info_by_3rd_party(self, identifier:str, driver: webdriver.Chrome) -> dict:
        counter_url = f'https://youtubelikecounter.com/#!/{identifier}'
        driver.get(counter_url)
        driver.refresh()
        time.sleep(5)
        mv_views, mv_likes, mv_comments = 0, 0, 0
        cnt = 0
        while cnt < 30:
            str_mv_views = driver.find_element(by=By.XPATH, value='//*[@id="__next"]/div/div[1]/div[1]').text
            str_mv_likes = driver.find_element(by=By.XPATH, value='//*[@id="__next"]/div/div[1]/div[2]').text
            str_mv_comments = driver.find_element(by=By.XPATH, value='//*[@id="__next"]/div/div[1]/div[4]').text
            
            mv_views = int(str_mv_views.replace('\n', '').replace(',', ''))
            mv_likes = int(str_mv_likes.replace('\n', '').replace(',', ''))
            mv_comments = int(str_mv_comments.replace('\n', '').replace(',', ''))

            print(identifier, cnt, mv_views, mv_likes, mv_comments)
            if 0 not in (mv_views, mv_likes, mv_comments):
                break
            time.sleep(1)
            cnt += 1

        return {
            'mv_identifier': identifier,
            'mv_views':mv_views,
            'mv_likes':mv_likes,
            'mv_comments':mv_comments,
        }

    @log_method_call
    def crawl_youtube_search(self, keyword_list: list) -> pd.DataFrame:
        meta_by_youtube = []
        driver = webdriver.Chrome(service=self.service, options=self.chrome_options)
        for _keyword in keyword_list:
            meta_by_youtube += [self._parse_content_info_by_youtube(keyword=_keyword, driver=driver)]
        driver.quit()
        meta_by_youtube = pd.DataFrame(meta_by_youtube)
        meta_by_youtube['is_official_channel'] = meta_by_youtube['mv_channel'].apply(lambda x: True if x in Scraper.official_channels['channel'].to_list() else False)

        return meta_by_youtube
    
    @log_method_call
    def crawl_content_info_by_3rd_party(self, identifier_list: list) -> pd.DataFrame:
        meta_by_3rd_party = []
        driver = webdriver.Chrome(service=self.service, options=self.chrome_options)

        for _identifier in identifier_list:
            meta_by_3rd_party += [self._parse_content_info_by_3rd_party(identifier=_identifier, driver=driver)]
        driver.quit()
        return pd.DataFrame(meta_by_3rd_party)
    
    @log_method_call
    def get_channel_img_url(self, channel: str, driver: webdriver.Chrome) -> str:
        channel_url = f'https://www.youtube.com/{channel}'
        driver.get(channel_url)
        xpath = '//*[@id="page-header"]/yt-page-header-renderer/yt-page-header-view-model/div/div[1]/yt-decorated-avatar-view-model/yt-avatar-shape/div/div/div/img'
        element = WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.XPATH, xpath)))
        return element.get_attribute('src')
    
    @log_method_call
    def update_channel_info_sheet(self, sheet='official_channels'):
        sheet = GSheetsConn(url=GOOGLE_SHEET_URL).get_worksheet(sheet='official_channels')

        # 크롤러로 이미지 파싱
        driver = webdriver.Chrome(service=self.service, options=self.chrome_options)
        img_dict = {}
        for channel in self.official_channels['channel'].unique():
            img_url = self.get_channel_img_url(channel=channel, driver=driver)
            img_dict[channel] = img_url
        driver.quit()

        # 기존 시트 형태의 dataframe에 맞춰 넣기
        for idx in self.official_channels.index:
            tmp_channel = self.official_channels.at[idx, 'channel']

            if self.official_channels.at[idx, 'img_url'] != img_dict[tmp_channel]:
                self.official_channels.at[idx, 'img_url'] = img_dict[tmp_channel]
                self.official_channels.at[idx, 'update_dt'] = cur.strftime('%Y-%m-%d %H:%M:%S')

        # 구글 시트 업데이트
        self.gs_cleint.update_google_sheet_column(self.official_channels, 'img_url', sheet)
        self.gs_cleint.update_google_sheet_column(self.official_channels, 'update_dt', sheet)
        return self.official_channels