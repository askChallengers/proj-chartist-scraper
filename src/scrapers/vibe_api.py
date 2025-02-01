from abc import ABC
from urllib.parse import urljoin
import pandas as pd
import requests
import xmltodict
from urllib.parse import urljoin

from src.config.helper import log_method_call

def requests_get_xml(url) -> dict:
    response = requests.get(url)
    xml_data = response.content
    return xmltodict.parse(xml_data)

class Vibe():
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
    def get_target_info_by_vibe(self, except_artists: pd.DataFrame, except_albums: pd.DataFrame, ranking:int=100):
        # top100 차트에서 가수 정보만 추출한다.
        chart_df = self.get_top100_chart()
        artist_info = chart_df.loc[
            chart_df['vibe_rank'].isin(range(ranking+1)) &\
            ~chart_df['artistId'].isin(except_artists['artistId']),
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

            tmp_block_album_list =  except_albums[lambda x: x['artistId'] == _artistId]['albumId'].to_list()
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
