from urllib.parse import urljoin
import pandas as pd
import re
import requests
import xmltodict
import pytz
from datetime import datetime
import re
import time
import random
from urllib.parse import urljoin
import inspect
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
from src.config.env import GOOGLE_SHEET_URL, EXECUTE_ENV
from .base_scraper import BaseScraper

# KST (Korea Standard Time) 시간대를 설정
kst = pytz.timezone('Asia/Seoul')
cur = datetime.now(kst)

class YoutubeScraper(BaseScraper):
    base_url = 'https://www.youtube.com'
    @log_method_call
    def __init__(self, is_headless: bool):
        super().__init__()
        self.chrome_options = webdriver.ChromeOptions()
        # 한국어 언어 설정
        self.chrome_options.add_argument("--lang=ko-KR")
        self.chrome_options.add_argument("Accept-Language=ko-KR")
        self.chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
        if EXECUTE_ENV == 'LOCAL':
            self.service = Service(ChromeDriverManager().install())
        else:
            self.service = None
            self.chrome_options.add_argument('--disable-dev-shm-usage') # 공유 메모리 사용하지 않도록 하는 옵션

        if is_headless:
            self.chrome_options.add_argument("--disable-blink-features=AutomationControlled")  # 자동화 탐지 방지
            self.chrome_options.add_argument("--no-sandbox") #샌드박스 모드 해제(보안 문제있을 수 있음)
            self.chrome_options.add_argument('window-size=1920x1080')
            self.chrome_options.add_argument("disable-gpu")
            self.chrome_options.add_argument('headless')

            # 자동화 감지 속성 제거
            self.chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            self.chrome_options.add_experimental_option("useAutomationExtension", False)

        else:
            self.chrome_options = None
    
    def _parse_content_count_info(self, mv_link: str, driver: webdriver.Chrome) -> dict:
        try:
            driver.get(mv_link)
            time.sleep(random.randint(3, 10))
            xpath_value = '//*[@id="watch7-content"]/meta[11]'
            WebDriverWait(driver, 30).until(EC.presence_of_all_elements_located((By.XPATH, xpath_value)))
            str_view_count = driver.find_element(by=By.XPATH, value=xpath_value).get_attribute('content')
            view_count = int(re.sub(r'[^0-9]', '', str_view_count))
            print(f'[_parse_content_count_info] view_count: {view_count}')
            return {'view_count': view_count}
        except TimeoutException:
            print('- TIMEOUT: GET THE FAKE KEYWORD')
            self.save_screenshot_to_gcs(function_name=inspect.currentframe().f_code.co_name, target_name='count_info_TimeoutException', driver=driver)
            driver.get('https://www.youtube.com/results?search_query=count_info')
            time.sleep(random.randint(3, 10))
        except Exception as e:
            self.save_screenshot_to_gcs(function_name=inspect.currentframe().f_code.co_name, target_name='count_info_e', driver=driver)
            raise e

    def _parse_channel_url(self, channel_href: str, driver: webdriver.Chrome):
        driver.get(channel_href)
        time.sleep(random.randint(3, 10))
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
            time.sleep(random.randint(3, 10))
            elements = WebDriverWait(driver, 90).until(EC.presence_of_all_elements_located((By.XPATH, '//*[@id="contents"]/ytd-video-renderer')))
            # 검색 후 최상단에 있는 것만 파싱해서 가져온다.
            elem = elements[0]
            specific_title_elem = elem.find_element(by=By.XPATH, value='.//*[@id="video-title"]')
            mv_title = specific_title_elem.get_attribute("title")
            mv_identifier = specific_title_elem.get_attribute("href")
            mv_identifier = mv_identifier.split('/watch?')[1].split('v=')[1].split('&')[0]

            mv_link = f'https://www.youtube.com/watch?v={mv_identifier}&hl=ko&gl=KR'
            channel = elem.find_element(by=By.XPATH, value='.//*[@id="channel-thumbnail"]').get_attribute("href")
            if not channel.startswith('@'):
                channel = self._parse_channel_url(channel_href=channel, driver=driver)
            channel = channel.replace('https://www.youtube.com/', '')
            view_count = None
            for _ in range(5):
                response = requests.get(f'https://api.socialcounts.org/youtube-video-live-view-count/{mv_identifier}', timeout=90)
                response.raise_for_status()
                if int(response.status_code) == 200:
                    view_count = response.json()['est_sub']
                print(f'youtube-video-live-view-count({mv_identifier}):', response.text)
                print('youtube_url:', mv_link)
                time.sleep(random.randint(3, 10))
            if view_count is None:
                raise Exception('youtube-video-live-view-count')
            # mv_count_info = self._parse_content_count_info(mv_link=mv_link, driver=driver)

            result = {
                'searchKeyword': keyword,
                'mv_channel': channel,
                'mv_identifier': mv_identifier,
                'mv_title': mv_title,
                'mv_link':mv_link,
                # 'view_count': mv_count_info['view_count'],
                'view_count': view_count,
                # 'comment_count': mv_count_info['comment_count'],
            }
            return result
        except TimeoutException:
            print('- TIMEOUT: GET THE FAKE KEYWORD')
            self.save_screenshot_to_gcs(function_name=inspect.currentframe().f_code.co_name, target_name=channel, driver=driver)
            driver.get('https://www.youtube.com/results?search_query=info_by_youtube')
            time.sleep(random.randint(3, 10))
        except Exception as e:
            self.save_screenshot_to_gcs(function_name=inspect.currentframe().f_code.co_name, target_name='e', driver=driver)
            raise e

    @log_method_call
    def crawl_youtube_search(self, keyword_list: list) -> pd.DataFrame:
        meta_by_youtube = []
        driver = webdriver.Chrome(service=self.service, options=self.chrome_options)
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {
                "source": """
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
                """
            }
        )

        retry_keyword_list = []
        for _keyword in keyword_list:
            try:
                meta_by_youtube += [self._parse_content_info_by_youtube(keyword=_keyword, driver=driver)]
            except:
                print(f'RETRY: {_keyword}')
                self.save_screenshot_to_gcs(function_name=inspect.currentframe().f_code.co_name, target_name=_keyword, driver=driver)
                retry_keyword_list += [_keyword]
        driver.quit()

        driver = webdriver.Chrome(service=self.service, options=self.chrome_options)
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {
                "source": """
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
                """
            }
        )

        for _keyword in retry_keyword_list:
            meta_by_youtube += [self._parse_content_info_by_youtube(keyword=_keyword, driver=driver)]
        driver.quit()
        meta_by_youtube = pd.DataFrame(meta_by_youtube)
        meta_by_youtube['is_official_channel'] = meta_by_youtube['mv_channel'].apply(lambda x: True if x in self.official_channels['channel'].to_list() else False)

        return meta_by_youtube
    
    @log_method_call
    def get_channel_img_url(self, channel: str, driver: webdriver.Chrome) -> str:
        channel_url = f'https://www.youtube.com/{channel}'
        driver.get(channel_url)
        time.sleep(random.randint(3, 10))
        xpath = '//*[@id="page-header"]/yt-page-header-renderer/yt-page-header-view-model/div/div[1]/yt-decorated-avatar-view-model/yt-avatar-shape/div/div/div/img'
        element = WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.XPATH, xpath)))
        return element.get_attribute('src')
    
    @log_method_call
    def save_screenshot_to_gcs(self, function_name:str, target_name: str, driver: webdriver.Chrome):
        png = driver.get_screenshot_as_png()
        clean_target_name = re.sub(r'[^\w\s.-]', '', target_name.replace(' ', '_'))
        storage_file_name = f'screenshot/{datetime.now(tz=kst).strftime("%Y%m%d%H%M")}_{function_name.upper()}_{clean_target_name}.png'
        # GCS 업로드
        self.gcs_client.upload_from_memory(
            contents=png,
            destination=storage_file_name,
            content_type='image/png'
        )

    @log_method_call
    def update_channel_info_sheet(self, sheet='official_channels'):
        sheet = GSheetsConn(url=GOOGLE_SHEET_URL).get_worksheet(sheet='official_channels')

        # 크롤러로 이미지 파싱
        driver = webdriver.Chrome(service=self.service, options=self.chrome_options)
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {
                "source": """
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
                """
            }
        )

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