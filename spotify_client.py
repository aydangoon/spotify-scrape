import asyncio
import base64
import aiohttp
import json

class SpotifyAPIConstants:
    BASE = 'https://api.spotify.com/v1'
    TOKEN_URL = 'https://accounts.spotify.com/api/token' 
    GENRE_SEEDS = 'genre_seeds'
    GENRE_SEEDS_PATH = '/recommendations/available-genre-seeds'
    ARTISTS = 'artists'
    ARTISTS_PATH = '/artists'
    RECOMMENDATIONS = 'recommendations'
    RECOMMENDATIONS_PATH = '/recommendations'
    ALBUMS = 'albums'
    ALBUMS_PATH = '/albums'
    CATEGORIES = 'categories'
    CATEGORIES_PATH = '/browse/categories'
    CATEGORY_PLAYLISTS = 'category_playlists'
    CATEGORY_PLAYLISTS_PATH = '/browse/categories/'
    PLAYLIST = 'playlist'
    PLAYLIST_PATH = '/playlists/' 
    ARTIST_RELATED_ARTISTS = 'artist_related_artists'
    ARTIST_RELATED_ARTISTS_PATH = '/artists/'
    SEARCH = 'search'
    SEARCH_PATH = '/search'
    RATE_LIMIT_CODE = 429
    OK_CODE = 200
    BAD_OAUTH_CODE = 403
    EXPIRED_TOKEN_CODE = 401
    PATHS = [ 
        GENRE_SEEDS, ARTISTS, RECOMMENDATIONS, ALBUMS, CATEGORIES,
        CATEGORY_PLAYLISTS, PLAYLIST, ARTIST_RELATED_ARTISTS, SEARCH
    ]

class SpotifyClient:
    def __init__(self, session: aiohttp.ClientSession):
        self.access_token = None
        self._session = session
        self._timeout = aiohttp.ClientTimeout(total=60)
        with open('key.json') as f:
            data = json.load(f)
            self._client_id = data['client_id']
            self._client_secret = data['client_secret']

    async def fetch(self, url, method, data, headers, params=None):
        try: 
            async with self._session.request(
                method, 
                url, 
                data=data, 
                headers=headers, 
                params=params, 
                timeout=self._timeout
            ) as response:
                status = response.status
                data = {} if response.content_type != 'application/json' else await response.json()
                if status != SpotifyAPIConstants.OK_CODE:
                    error = data.get('error', { 'message': 'No error message provided'})
                    print(f"Failed to fetch {url}. Error {status}: {error['message']}")
                if status == SpotifyAPIConstants.RATE_LIMIT_CODE:
                    retry_after = response.headers.get('Retry-After')
                    if retry_after is not None:
                        data['retry_after'] = float(retry_after)
                return { 'status': status, 'data': data }
        except (asyncio.TimeoutError, aiohttp.ClientError) as e:
            print(f"An error occurred during the request: {str(e)}")
            return None

    async def refresh_access_token(self):
        auth_header = base64.b64encode(f"{self._client_id}:{self._client_secret}".encode()).decode()
        data = { 'grant_type': 'client_credentials' }
        headers = {
            'Authorization': f"Basic {auth_header}",
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        res = await self.fetch(SpotifyAPIConstants.TOKEN_URL, 'POST', data, headers)
        if res is None:
            raise Exception("Failed to refresh access token")
        self.access_token = res['data']['access_token']