import base64
import aiohttp
import json

async def initialize():
    global CLIENT_ID, CLIENT_SECRET, ACCESS_TOKEN
    with open('key.json') as f:
        data = json.load(f)
        CLIENT_ID = data['client_id']
        CLIENT_SECRET = data['client_secret']
    ACCESS_TOKEN = await get_access_token()
    if ACCESS_TOKEN is None:
        raise Exception("Failed to obtain access token")
    print('access_token:', ACCESS_TOKEN)

async def get_access_token():
    auth_header = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    data = {
        'grant_type': 'client_credentials'
    }
    headers = {
        'Authorization': f"Basic {auth_header}",
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    
    async with aiohttp.ClientSession() as session:
        try: 
            async with session.post('https://accounts.spotify.com/api/token', data=data, headers=headers) as response:
                if response.status == 200:
                    response_data = await response.json()
                    access_token = response_data.get('access_token')
                    return access_token
                else:
                    print(f"Failed to obtain access token. Error {response.status}: {response.reason}")
                    return None
        except aiohttp.ClientError as e:
            print(f"An error occurred during the request: {str(e)}")
            return None