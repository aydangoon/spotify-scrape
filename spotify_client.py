import base64
import aiohttp
import json

BASE = 'https://api.spotify.com/v1'

async def initialize(session):
    global CLIENT_ID, CLIENT_SECRET, ACCESS_TOKEN
    with open('key.json') as f:
        data = json.load(f)
        CLIENT_ID = data['client_id']
        CLIENT_SECRET = data['client_secret']
    ACCESS_TOKEN = await get_access_token(session)
    if ACCESS_TOKEN is None:
        raise Exception("Failed to obtain access token")
    print('access_token:', ACCESS_TOKEN)

async def fetch(session: aiohttp.ClientSession, url, method, data, headers):
    try: 
        async with session.request(method, url, data=data, headers=headers) as response:
            status = response.status
            data = await response.json()
            if status != 200:
                print(f"Failed to fetch {url}. Error {status}: {data.error.message}")
            if status == 429:
                retry_after = response.headers.get('Retry-After')
                if retry_after is not None:
                    data['retry_after'] = float(retry_after)
            return { 'status': status, 'data': data }
    except aiohttp.ClientError as e:
        print(f"An error occurred during the request: {str(e)}")
        return None

async def get_access_token(session):
    auth_header = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    data = {
        'grant_type': 'client_credentials'
    }
    headers = {
        'Authorization': f"Basic {auth_header}",
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    url = 'https://accounts.spotify.com/api/token' 
    res = await fetch(session, url, 'POST', data, headers)
    if res is None:
        return None
    return res['data']['access_token']