from abc import ABC
from urllib.parse import urljoin
import pandas as pd
import requests
import xmltodict

import time
from urllib.parse import urljoin

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from src.auth import GCPAuth
from src.config.env import GOOGLE_SHEET_URL
def requests_get_xml(url) -> dict:
    response = requests.get(url)
    xml_data = response.content
    return xmltodict.parse(xml_data)

class Scraper(ABC):
    # 클래스 변수
    block_albums = GCPAuth(url=GOOGLE_SHEET_URL).get_df_from_google_sheets(sheet='block_albums')
    official_channels = GCPAuth(url=GOOGLE_SHEET_URL).get_df_from_google_sheets(sheet='official_channels')
    
    def __init__(self):
        self.official_channels.replace('', None, inplace=True)
        self.official_channels['artistId'] = pd.to_numeric(self.official_channels['artistId'])
        self.block_albums['artistId'] = pd.to_numeric(self.block_albums['artistId']) 
        self.block_albums['albumId'] = pd.to_numeric(self.block_albums['albumId'])
    
class VibeScraper(Scraper):
    base_url = 'https://apis.naver.com'

    def __init__(self):
        super().__init__()

    def get_top100_chart(self) -> pd.DataFrame:
        end_point = 'vibeWeb/musicapiweb/vibe/v1/chart/track/genres/DS101?start=1&display=100'
        url = urljoin(self.base_url, end_point)
        dict_data = requests_get_xml(url)
        track_list = dict_data['response']['result']['chart']['items']['tracks']['track']
        result = pd.DataFrame(columns=['rank', 'trackTitle', 'artistName', 'artistId'])
        for _r, _track in enumerate(track_list):
            trackTitle = _track['trackTitle']
            if isinstance(_track['artists']['artist'], list):
                artistName_list = list(map(lambda x: x['artistName'], _track['artists']['artist']))
                artistId_list = list(map(lambda x: x['artistId'], _track['artists']['artist']))
                artistName, artistId = artistName_list[0], artistId_list[0]
            else:
                artistName = _track['artists']['artist']['artistName']
                artistId = _track['artists']['artist']['artistId']
            row = pd.DataFrame([{'rank': _r+1, 'trackTitle': trackTitle, 'artistName': artistName, 'artistId': artistId}])
            result = pd.concat([result, row], ignore_index=True)
        result['artistId'] = result['artistId'].astype(int)
        return result

    def get_latest_album_info_by_artistId(self, artistId: int, block_albumIds: list) -> pd.DataFrame:
        end_point = 'vibeWeb/musicapiweb/v3/musician/artist/<artistId>/albums?start=1&display=10&type=ALL&sort=newRelease'.replace('<artistId>', str(artistId))
        url = urljoin(self.base_url, end_point)
        dict_data = requests_get_xml(url)
        
        for _album in dict_data['response']['result']['albums']['album']:
            # 제외: 협업 OR 자체블락
            specific_info = self.get_specific_album_info(_album['albumId'])
            if specific_info['albumGenres'] == 'J-팝' or specific_info['artistTotalCount'] > 1 or int(_album['albumId']) in block_albumIds:
                continue
            track_list_df = self.get_tracks_info_by_albumId(_album['albumId'])
            latest_album = pd.DataFrame([_album])[['albumId', 'albumTitle', 'releaseDate', 'imageUrl']]
            result = latest_album.merge(track_list_df, on='albumId', how='left')
            break
        result['artistId'] = artistId
        result['albumId'] = result['albumId'].astype(int)
        return result

    def get_specific_album_info(self, albumId: int) -> dict:
        end_point = 'vibeWeb/musicapiweb/album/<albumId>?includeDesc=true&includeIntro=true'.replace('<albumId>', str(albumId))
        url = urljoin(self.base_url, end_point)
        dict_data = requests_get_xml(url)
        album_info = dict_data['response']['result']['album']
        album_info['artistTotalCount'] = int(album_info['artistTotalCount'])
        return album_info
        
    def get_tracks_info_by_albumId(self, albumId: int) -> pd.DataFrame:
        end_point = 'vibeWeb/musicapiweb/album/<albumId>/tracks?start=1&display=1000'.replace('<albumId>', albumId)
        url = urljoin(self.base_url, end_point)
        dict_data = requests_get_xml(url)
        result = pd.DataFrame(dict_data['response']['result']['tracks']['track'])
        result[['trackId', 'likeCount']] = result[['trackId', 'likeCount']].astype(int)
        result[['represent', 'isOversea', 'isTopPopular']] = result[['represent', 'isOversea', 'isTopPopular']].astype(bool)
        result['albumId'] = albumId
        return result[
            ['trackId', 'trackTitle', 'represent', 'albumId', 'isOversea', 'likeCount', 'score', 'isTopPopular']
        ]
    
    def get_target_info_by_vibe(self):
        # top100 차트에서 가수 정보만 추출한다.
        chart_df = self.get_top100_chart()
        chart_df.loc[chart_df['rank'].isin(range(100)), ['artistId', 'artistName']].drop_duplicates()
        # 구글 시트에 저장된 타겟 가수의 정보를 가져와서 붙인다.
        artist_info = chart_df[['artistId', 'artistName']].drop_duplicates().merge(
            Scraper.official_channels.drop(columns=['artistName']),
            on='artistId',
            how='inner',
        )

        # 각 타겟 가수별 제외 대상 앨범 외의 최신 앨범 정보를 가져온다.
        latest_album_info_by_artistId = []
        for _artistId in artist_info['artistId'].unique():
            tmp_block_album_list =  Scraper.block_albums[lambda x: x['artistId'] == _artistId]['albumId'].to_list()
            tmp = self.get_latest_album_info_by_artistId(_artistId, tmp_block_album_list)
            latest_album_info_by_artistId += [tmp]
        latest_album_info_by_artistId = pd.concat(latest_album_info_by_artistId).reset_index(drop=True)

        # 타겟 가수별 최신 앨범 정보를 가수 정보에 붙인다.
        total_info = artist_info.merge(latest_album_info_by_artistId, on='artistId', how='left')

        # 가수:앨범:노래=1:1:1로 하기 위해, 타이틀곡에 대해서만 가져오되, 멀티 타이틀인 경우, 그 중 likeCount 높은 걸 가져온다.
        total_info = total_info[total_info['represent'] == True]\
            .sort_values(by=['likeCount'], ascending=False)\
            .drop_duplicates(subset=['artistId'], keep='first')[
            ['artistName', 'channel', 'trackTitle', 'albumTitle']
        ].reset_index(drop=True)

        return total_info

class YoutubeScraper(Scraper):
    base_url = 'https://www.youtube.com'

    def __init__(self):
        super().__init__()

    def _parse_content_info_by_youtube(self, keyword: str, driver: webdriver.Chrome) -> dict:
        end_point = f'results?search_query={keyword}'
        url = urljoin(self.base_url, end_point)
        driver.get(url)
        
        elements = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.XPATH, '//*[@id="contents"]/ytd-video-renderer')))
        # 검색 후 최상단에 있는 것만 파싱해서 가져온다.
        elem = elements[0]
        specific_title_elem = elem.find_element(by=By.XPATH, value='.//*[@id="video-title"]')
        mv_channel = elem.find_element(by=By.XPATH, value='.//*[@id="channel-thumbnail"]').get_attribute("href")
        mv_channel = mv_channel.replace('https://www.youtube.com/', '')
        
        mv_title = specific_title_elem.get_attribute("title")
        mv_identifier = specific_title_elem.get_attribute("href")
        mv_identifier = mv_identifier.split('/watch?')[1].split('v=')[1].split('&')[0]
        mv_link = f'https://www.youtube.com/watch?v={mv_identifier}'
        
        return {
            'searchKeyword': keyword,
            'mv_identifier': mv_identifier,
            'mv_title': mv_title,
            'mv_channel': mv_channel,
            'mv_link':mv_link,
        }

    def _parse_content_info_by_3rd_party(self, identifier:str, driver: webdriver.Chrome) -> dict:
        counter_url = f'https://youtubelikecounter.com/#!/{identifier}'
        driver.get(counter_url)
        driver.refresh()
        time.sleep(5)
        mv_views = driver.find_element(by=By.XPATH, value='//*[@id="__next"]/div/div[1]/div[1]').text
        mv_likes = driver.find_element(by=By.XPATH, value='//*[@id="__next"]/div/div[1]/div[2]').text
        mv_comments = driver.find_element(by=By.XPATH, value='//*[@id="__next"]/div/div[1]/div[4]').text

        mv_views = int(mv_views.replace('\n', '').replace(',', ''))
        mv_likes = int(mv_likes.replace('\n', '').replace(',', ''))
        mv_comments = int(mv_comments.replace('\n', '').replace(',', ''))
        
        return {
            'mv_identifier': identifier,
            'mv_views':mv_views,
            'mv_likes':mv_likes,
            'mv_comments':mv_comments,
        }

    def crawl_youtube_search(self, keyword_list: list) -> pd.DataFrame:
        meta_by_youtube = []
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))

        for _keyword in keyword_list:
            meta_by_youtube += [self._parse_content_info_by_youtube(keyword=_keyword, driver=driver)]
        driver.quit()
        meta_by_youtube = pd.DataFrame(meta_by_youtube)

        # 크롤링 된 데이터가 official 채널인지 확인
        sub_channels_map = { 
            Scraper.official_channels.at[i, 'sub_channel'] : Scraper.official_channels.at[i, 'channel']
            for i in Scraper.official_channels.loc[Scraper.official_channels['sub_channel'] != '', ['channel', 'sub_channel']].index
        }
        meta_by_youtube['mv_channel'] = meta_by_youtube['mv_channel'].apply(lambda x: sub_channels_map[x] if x in sub_channels_map.keys() else x)
        meta_by_youtube['is_official_channel'] = meta_by_youtube['mv_channel'].apply(lambda x: True if x in Scraper.official_channels['channel'].to_list() else False)

        return meta_by_youtube
    
    def crawl_content_info_by_3rd_party(self, identifier_list: list) -> pd.DataFrame:
        meta_by_3rd_party = []
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))

        for _identifier in identifier_list:
            meta_by_3rd_party += [self._parse_content_info_by_3rd_party(identifier=_identifier, driver=driver)]
        driver.quit()
        return pd.DataFrame(meta_by_3rd_party)
