import base64
import aiohttp
import json

BASE = 'https://api.spotify.com/v1'
STATIC_PATHS = {
    'genre_seeds': '/recommendations/available-genre-seeds',
    'artists': '/artists',
    'recommendations': '/recommendations',
    'albums': '/albums',
    'categories': '/browse/categories',
    'artist_related_artists': '/artists/',
}
STATUS_CODES = {
    'OK': 200,
    'RATE_LIMITED': 429,
    'BAD_OAUTH': 403,
    'OLD_ACCESS_TOKEN': 401,
}

class SpotifyClient:
    def __init__(self, session: aiohttp.ClientSession):
        self.access_token = None
        self._session = session
        with open('key.json') as f:
            data = json.load(f)
            self._client_id = data['client_id']
            self._client_secret = data['client_secret']

    async def fetch(self, url, method, data, headers):
        try: 
            async with self._session.request(method, url, data=data, headers=headers) as response:
                status = response.status
                data = {} if response.content_type != 'application/json' else await response.json()
                if status != STATUS_CODES['OK']:
                    error = data.get('error', { 'message': 'No error message provided'})
                    print(f"Failed to fetch {url}. Error {status}: {error['message']}")
                if status == STATUS_CODES['RATE_LIMITED']:
                    print('RAW RESPONSE')
                    text = await response.text()
                    print('text:', text)
                    retry_after = response.headers.get('Retry-After')
                    print('retry_after:', retry_after)
                    if retry_after is not None:
                        data['retry_after'] = float(retry_after)
                return { 'status': status, 'data': data }
        except aiohttp.ClientError as e:
            print(f"An error occurred during the request: {str(e)}")
            return None

    async def refresh_access_token(self):
        auth_header = base64.b64encode(f"{self._client_id}:{self._client_secret}".encode()).decode()
        data = {
            'grant_type': 'client_credentials'
        }
        headers = {
            'Authorization': f"Basic {auth_header}",
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        url = 'https://accounts.spotify.com/api/token' 
        res = await self.fetch(url, 'POST', data, headers)
        if res is None:
            raise Exception("Failed to refresh access token")
        self.access_token = res['data']['access_token']
        print('access_token:', self.access_token)