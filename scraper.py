from typing import List
import time
import traceback
import argparse
import asyncio
import aiohttp
import redis.asyncio as redis
from cache import Cache
from artists_writer import ArtistsWriter
from backoff_policy import BackoffPolicy
from batch_req_builder import BatchReqBuilder
from path_queues import PathQueues
from spotify_client import SpotifyClient, BASE, STATIC_PATHS, STATUS_CODES

RATE_LIMIIT_SAFETY = 5 # temp. if we get this number of 429s, just stop
SCORE_UPDATE_RATE = 20 # every 20 calls update the priority queue
FLUSH_RATE = 50 # every 50 calls flush the queue

class Scraper:
    def __init__(self, seed: List[str], session: aiohttp.ClientSession):
        self._cache = Cache(fresh=FRESH)
        self._backoff_policy = BackoffPolicy()
        self._artists_writer = ArtistsWriter(fresh=FRESH)
        self._sc = SpotifyClient(session)
        self._seed = seed
        self._total = 0
        self._rate_limit_hits = 0
        self._batch_artist_req_builder = BatchReqBuilder(size=50)
        self._queue = asyncio.Queue()
        self._path_queues = PathQueues()

        # data collection
        self._route_data = { key: { 'time': 0, 'calls': 0, 'batched': 0, 'added': 0 } for key in STATIC_PATHS.keys() }

    async def flush_path_queues(self):
        endpoints = await self._path_queues.flush()
        print(f'[Scraper]: flushing {len(endpoints)} paths')
        for endpoint in endpoints:
            await self._queue.put(endpoint)
    
    async def sort_path_queues(self):
        scores = {}
        for path in STATIC_PATHS.keys():
            data = self._route_data[path]
            info = data['added'] + 0.5*data['batched']
            calls = data['calls']
            scores[path] = 0 if calls == 0 else info/calls
        print('[Scraper]: setting priorities to:', scores)
        await self._path_queues.set_priority(scores)


    async def run(self):
        await self._sc.refresh_access_token()
        for seed in self._seed:
            await self._queue.put(seed)

        print('[Scraper]: Starting Scraper...')
        print('[Scraper]: Initial Queue Size:', self._queue.qsize())
        start = time.time()
        workers = [asyncio.create_task(self.worker()) for _ in range(NUM_WORKERS)]

        while not self._path_queues.empty() or not self._queue.empty():
            if self._queue.empty():
                await self.flush_path_queues()
            await self._queue.join()
        print('[Scraper]: Queue has no unfinished tasks. Cancelling workers...')

        for w in workers:
            w.cancel()
        
        print(f'[Scraper]: finished in {time.time() - start} seconds')
        print('[Scraper] Route Stats:')
        for key, val in self._route_data.items():
            print("======================================================")
            print(key+":")
            time_per_call = "N/A" if val['calls'] == 0 else val['time']/val['calls']
            info_per_call = "N/A" if val['calls'] == 0 else (val['added'] + 0.5*val['batched'])/val['calls']
            info_per_sec = "N/A" if time_per_call == 0 or time_per_call == "N/A" or info_per_call=="N/A" else info_per_call/time_per_call
            score = "N/A" if val['calls'] == 0 or val['time'] == 0 else (val['added'] + 0.5*val['batched'])/(val['time']*val['calls'])
            print(f"\tTotal Time: {val['time']}")
            print(f"\tTime per call: {time_per_call}")
            print(f"\tInfo per call: {info_per_call}")
            print(f"\tInfo per second: {info_per_sec}")
            print(f"\tScore: {score}")
            print("======================================================")

    async def worker(self):
        while True:
            try:
                await self.process_one()
            except asyncio.CancelledError:
                return
    
    async def process_one(self):
        endpoint = await self._queue.get()
        try:
            if self._total < MAX_NUM_ARTISTS:
                await self.process_endpoint(endpoint)
        except Exception as e:
            print(f'Error processing endpoint {endpoint}: {e}')
            traceback.print_exc()
        finally:
            self._queue.task_done()
            if self._total != 0:
                if self._total % SCORE_UPDATE_RATE == 0:
                    await self.sort_path_queues()
                if self._total % FLUSH_RATE == 0:
                    await self.flush_path_queues()
    
    async def process_endpoint(self, endpoint):

        # print('Processing:', endpoint)

        if self._rate_limit_hits >= RATE_LIMIIT_SAFETY:
            print('[Rate Limit]: safety hit. Stopping...')
            return
        
        # rate limit
        wait_sec = await self._backoff_policy.get_backoff()
        if wait_sec > 0:
            print(f'[Rate Limit]: Waiting {wait_sec} seconds...')
            await asyncio.sleep(wait_sec)
        
        # attempt to fetch
        headers = {'Authorization': f"Bearer {self._sc.access_token}"}
        start_time = time.time()
        res = await self._sc.fetch(
            url=BASE+endpoint['path'],
            method='GET',
            data=None,
            headers=headers,
            params=endpoint['params']
        )
        call_time = time.time() - start_time

        endpoint_str = endpoint['path'] + "| params: " + str(endpoint['params'])
        # route cache check
        # if await self._cache.exists(endpoint_str):
        #     print('VERY BAD: send duplicate request')
        # await self._cache.set(endpoint_str, b'1')
        endpoint_str_pretty = endpoint_str[:100]+"..." if len(endpoint_str) > 100 else endpoint_str

        print(f'Response: {"XXX" if res is None else res["status"]} | Endpoint: {endpoint_str_pretty}')

        # handle response
        if res is None: # connection or other severe error
            # TODO: better handling? do we just requeue?
            await self._queue.put(endpoint)
        elif res['status'] == STATUS_CODES['RATE_LIMITED']:
            print('[Rate Limit]: warning')
            self._rate_limit_hits += 1
            retry_after = res['data']['retry_after']
            await self._backoff_policy.set_retry_after(retry_after)
            await self._backoff_policy.incr_attempts()
            await self._queue.put(endpoint)
        elif res['status'] == STATUS_CODES['OLD_ACCESS_TOKEN']:
            print("Refreshing access token...")
            await self._sc.refresh_access_token()
            await self._queue.put(endpoint)
        elif res['status'] == STATUS_CODES['BAD_OAUTH']:
            print('Bad OAuth token')
        else: # success
            await self.process_valid_endpoint(endpoint, res['data'], call_time)
    
    async def process_valid_endpoint(self, endpoint, data, call_time):
        path = endpoint['path']
        [added, batched] = [0, 0]
        if path.startswith(STATIC_PATHS['genre_seeds']):
            await self.process_genres(data['genres'])
            route = 'genre_seeds'
        elif path.startswith(STATIC_PATHS['recommendations']):
            [a0, b0] = await self.process_artists([artist for track in data['tracks'] for artist in track["artists"]])
            [a1, b1] = await self.process_albums([track['album'] for track in data['tracks']])
            added += a0 + a1
            batched += b0 + b1
            route = 'recommendations'
        elif path.startswith(STATIC_PATHS['artist_related_artists']):
            [a0, b0] = await self.process_artists(data['artists'])
            added += a0
            batched += b0
            route = 'artist_related_artists'
        elif path.startswith(STATIC_PATHS['artists']):
            [a0, b0] = await self.process_artists(data['artists'])
            added += a0
            batched += b0
            route = 'artists'
        elif path.startswith(STATIC_PATHS['albums']):
            [a0, b0] = await self.process_albums(data['albums'])
            added += a0
            batched += b0
            route = 'albums'
        elif path.startswith(STATIC_PATHS['category_playlists']):
            print("Processing category playlists...")
            await self.process_category_playlists(data['playlists'])
            route = 'category_playlists'
        elif path.startswith(STATIC_PATHS['categories']):
            print("Processing categories...")
            await self.process_categories(data['categories'])
            route = 'categories'
        elif path.startswith(STATIC_PATHS['playlist']):
            print("Processing playlist...")
            [a0, b0] = await self.process_playlist(data)
            added += a0
            batched += b0
            route = 'playlist'
        elif path.startswith(STATIC_PATHS['search']):
            [a0, b0] = await self.process_search(data.get('artists', []))
            added += a0
            batched += b0
            route = 'search'

        self._route_data[route]['time'] += call_time
        self._route_data[route]['calls'] += 1
        self._route_data[route]['added'] += added
        self._route_data[route]['batched'] += batched
    
    async def process_search(self, artists):
        [a0, b0] = await self.process_artists(artists['items'])
        if artists.get('next', None) is not None:
            next_str = artists['next']
            v1_idx = next_str.index('v1')
            path = next_str[v1_idx+2:]
            # print('enqueuing next search', path)
            await self._path_queues.put('search', { 'path': path, 'params': None })
        return [a0, b0]

    async def process_playlist(self, playlist):
        tracks = [item['track'] for item in playlist['items'] if item['track']['type'] == 'track']
        [a0, b0] = await self.process_artists([artist for track in tracks for artist in track['artists']])
        [a1, b1] = await self.process_albums([track['album'] for track in tracks])
        if 'next' in playlist and playlist['next'] is not None:
            next_str = playlist['next']
            v1_idx = next_str.index('v1')
            path = next_str[v1_idx+2:]
            await self._path_queues.put('playlist', { 'path': path, 'params': None })
        return [a0 + a1, b0 + b1]

    async def process_category_playlists(self, playlists):
        for playlist in playlists['items']:
            if playlist is None or await self._cache.exists(playlist['id']):
                continue
            await self._cache.set(playlist['id'], b'1')
            await self._path_queues.put('playlist', { 
                'path': f"/playlists/{playlist['id']}/tracks", 
                'params': { 
                    'limit': 50,
                    'fields': 'next,items(track(type,album(id),artists(id)))'
                } 
            })
        if 'next' in playlists and playlists['next'] is not None:
            next_str = playlists['next']
            v1_idx = next_str.index('v1')
            path = next_str[v1_idx+2:]
            await self._path_queues.put('category_playlists',{ 'path': path, 'params': None })

    async def process_categories(self, categories):
        for category in categories['items']:
            if await self._cache.exists(category['id']):
                continue
            await self._cache.set(category['id'], b'1')
            await self._path_queues.put('category_playlists', { 
                'path': f"/browse/categories/{category['id']}/playlists", 
                'params': { 'limit': 50 } 
            })
        if 'next' in categories and categories['next'] is not None:
            next_str = categories['next']
            v1_idx = next_str.index('v1')
            path = next_str[v1_idx+2:]
            await self._path_queues.put('categories', { 'path': path, 'params': None })

    async def process_genres(self, genres, artist_id=None):
        for genre in genres:
            if await self._cache.exists(genre):
                continue
            params = {'seed_genres': genre, 'limit': 100}
            if artist_id is not None:
                params['seed_artists'] = artist_id
            await self._path_queues.put('recommendations', { 'path': '/recommendations', 'params': params })
            await self._path_queues.put('search', { 'path': '/search', 'params': { 
                'q': 'genre:'+genre,
                'type': 'artist',
                'limit': 50 
            }})
            await self._cache.set(genre, b'1')
    
    async def process_albums(self, albums):
        for album in albums:
            if await self._cache.exists(album['id']):
                continue
            await self._cache.set(album['id'], b'1')
        return await self.process_artists([artist for album in albums for artist in album.get('artists', [])])

    async def process_artists(self, artists):
        print(f'[PROCESSING]: {len(artists)}')
        added_artists = 0
        batched_artists = 0
        for artist in artists:
            artist_id = artist.get('id')
            if self._total >= MAX_NUM_ARTISTS or artist_id is None: 
                break

            cache_val = await self._cache.get(artist_id)
            if cache_val == b'2':
                continue

            genres, popularity, name = artist.get('genres'), artist.get('popularity'), artist.get('name')
            missing_data = genres is None or popularity is None or name is None

            if missing_data and cache_val == b'1':
                continue
            elif missing_data:
                await self._cache.set(artist_id, b'1')
                await self._batch_artist_req_builder.add(artist_id)
                batched_artists += 1
                # print(f'Added {artist_id} to batch request')
                if await self._batch_artist_req_builder.is_full():
                    # print("Batch request is full, enqueing batch request...")
                    await self._path_queues.put('artists', {
                        'path': '/artists',
                        'params': {'ids': await self._batch_artist_req_builder.build()}
                    })
            else:
                await self._artists_writer.add(id=artist_id, name=name, popularity=popularity, genres=genres)
                added_artists += 1 
                await self._cache.set(artist_id, b'2')
                # print(f'Added {artist_id} to artists.csv')
                self._total += 1
                if self._total >= MAX_NUM_ARTISTS:
                    print('Reached max number of artists. Stopping...')
                    break
                await self._path_queues.put('artist_related_artists', { 
                    'path': f"/artists/{artist_id}/related-artists", 
                    'params': None 
                })
                await self.process_genres(genres, artist_id)
        print(f'[ADDED]: {added_artists} [BATCHED]: {batched_artists}')
        return [added_artists, batched_artists]
    
async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-n", "--max-num-artists", type=int, help="Set the max number of artists to scrape")
    parser.add_argument('-d', '--debug', action='store_true', help='Enable debug mode')
    parser.add_argument('-f', '--fresh', action='store_true', help='Start with a fresh cache and empty artists.csv')
    parser.add_argument("-w", "--num-workers", type=int, help="Set the number of workers to use")
    args = parser.parse_args()
    global DEBUG, MAX_NUM_ARTISTS, FRESH, NUM_WORKERS
    DEBUG = bool(args.debug)
    FRESH = bool(args.fresh)
    MAX_NUM_ARTISTS = args.max_num_artists or 100
    if not args.num_workers:
        if MAX_NUM_ARTISTS <= 200:
            default_num_workers = 10
        else:
            default_num_workers = 20
    NUM_WORKERS = args.num_workers or default_num_workers
    print(f"Debug mode: {DEBUG}")

    # Seeds for testing
    SEVERAL_ARTISTS = {
        'path': '/artists',
        'params': {
            'ids': '2CIMQHirSU0MQqyYHq0eOx,57dN52uHvrHOxijzpIgu3E,1vCWHaC5f2uS3yhpwWbIA6'
        }
    }
    RELATED_ARTISTS = { 'path':'/artists/0TnOYISbd1XYRBk9myaseg/related-artists', 'params': None }
    SEVERAL_ALBUMS = {
        'path': '/albums', 
        'params':{
            'ids':'382ObEPsp2rxGrnsizN5TX,1A2GTWGtFfWp7KSQTwWOyo,2noRn2Aes5aoNVsU6iWThc'
        }
    }
    GENRE_SEEDS = { 'path': '/recommendations/available-genre-seeds', 'params': None }
    PLAYLIST = { 'path': '/playlists/3cEYpjA9oz9GiPac4AsH4n/tracks', 'params': None }
    CATEGORY_PLAYLISTS = { 'path' : '/browse/categories/dinner/playlists', 'params': None}
    CATEGORY = { 'path': '/browse/categories', 'params': None}
    SEARCH = { 'path': '/search?query=genre%3Arock&type=artist&offset=20&limit=50', 'params': None }

    async with aiohttp.ClientSession() as session:
        scraper = Scraper(seed=[GENRE_SEEDS, CATEGORY], session=session)
        await scraper.run() 

if __name__ == "__main__":
    asyncio.run(main())