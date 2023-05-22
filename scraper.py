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
from spotify_client import SpotifyClient, BASE, STATIC_PATHS, STATUS_CODES

RATE_LIMIIT_SAFETY = 5 # temp. if we get this number of 429s, just stop

class Scraper:
    def __init__(self, seed: List[str], session: aiohttp.ClientSession):
        self._cache = Cache(fresh=FRESH)
        self._backoff_policy = BackoffPolicy()
        self._artists_writer = ArtistsWriter(fresh=FRESH)
        self._sc = SpotifyClient(session)
        # queue for batch artist requests. These take priority over other requests
        self._primary_queue = asyncio.Queue()
        # queue for all other requests
        self._secondary_queue = asyncio.Queue()
        self._seed = seed
        self._total = 0
        self._rate_limit_hits = 0
        self._batch_artist_req_builder = BatchReqBuilder(size=50)

        # data collection
        self._route_data = { key: { 'time': 0, 'calls': 0, 'batched': 0, 'added': 0 }for key in STATIC_PATHS.keys() }

    async def run(self):
        await self._sc.refresh_access_token()
        for endpoint in self._seed:
            await self._primary_queue.put(endpoint)

        print('[Scraper]: Starting Scraper...')
        print('[Scraper]: Initial Queue Size:', self._secondary_queue.qsize())
        start = time.time()
        workers = [asyncio.create_task(self.worker()) for _ in range(NUM_WORKERS)]

        while not self._primary_queue.empty():
            await self._primary_queue.join()
            await self._secondary_queue.join()
        print('[Scraper]: Queue has no unfinished tasks. Cancelling workers...')

        for w in workers:
            w.cancel()
        
        print(f'[Scraper]: finished in {time.time() - start} seconds')
        print('[Scraper] Route Stats:')
        for key, val in self._route_data.items():
            print("======================================================")
            print(key+":")
            print(f"\tTotal Time: {val['time']}")
            time_per_call = "N/A" if val['calls'] == 0 else val['time']/val['calls']
            print(f"\tTime per call: {time_per_call}")
            added_per_sec = "N/A" if val['time'] == 0 else val['added']/val['time']
            print(f"\tAdded per second: {added_per_sec}")
            batched_per_sec = "N/A" if val['time'] == 0 else val['batched']/val['time']
            print(f"\tBatched per second: {batched_per_sec}")
            print("======================================================")

    async def worker(self):
        while True:
            try:
                await self.process_one()
            except asyncio.CancelledError:
                return
    
    async def process_one(self):
        processing_primary = not self._primary_queue.empty()
        if processing_primary:
            endpoint = await self._primary_queue.get()
        else:
            endpoint = await self._secondary_queue.get()

        try:
            if self._total < MAX_NUM_ARTISTS:
                await self.process_endpoint(endpoint)
        except Exception as e:
            print(f'Error processing endpoint {endpoint}: {e}')
            traceback.print_exc()
        finally:
            if processing_primary:
                self._primary_queue.task_done()
            else:
                self._secondary_queue.task_done()
    
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
            await self._secondary_queue.put(endpoint)
        elif res['status'] == STATUS_CODES['RATE_LIMITED']:
            print('[Rate Limit]: warning')
            self._rate_limit_hits += 1
            retry_after = res['data']['retry_after']
            await self._backoff_policy.set_retry_after(retry_after)
            await self._backoff_policy.incr_attempts()
            await self._secondary_queue.put(endpoint)
        elif res['status'] == STATUS_CODES['OLD_ACCESS_TOKEN']:
            print("Refreshing access token...")
            await self._sc.refresh_access_token()
            await self._secondary_queue.put(endpoint)
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
        # TODO: categories and playlists
        self._route_data[route]['time'] += call_time
        self._route_data[route]['calls'] += 1
        self._route_data[route]['added'] += added
        self._route_data[route]['batched'] += batched

    async def process_genres(self, genres, artist_id=None):
        for genre in genres:
            if await self._cache.exists(genre):
                continue
            params = {'seed_genres': genre, 'limit': 100}
            if artist_id is not None:
                params['seed_artists'] = artist_id
            await self._secondary_queue.put({ 'path': '/recommendations', 'params': params })
            await self._cache.set(genre, b'1')
    
    async def process_albums(self, albums):
        for album in albums:
            if await self._cache.exists(album['id']):
                continue
            await self._cache.set(album['id'], b'1')
        return await self.process_artists([artist for album in albums for artist in album.get('artists', [])])
    
    def get_unique_dicts_with_id(self, lst):
        unique_ids = set()
        unique_dicts = []
        
        for d in lst:
            if 'id' in d and d['id'] not in unique_ids:
                unique_ids.add(d['id'])
                unique_dicts.append(d)
        
        return len(unique_dicts)

    async def process_artists(self, artists):
        print(f'processing {len(artists)} artists')
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
                    print("Batch request is full, enqueing batch request...")
                    await self._primary_queue.put({
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
                await self._secondary_queue.put({ 'path': f"/artists/{artist_id}/related-artists", 'params': None })
                await self.process_genres(genres, artist_id)
        print(f'added {added_artists} artists, batched {batched_artists} artists')
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
    RELATED_ARTISTS = '/artists/0TnOYISbd1XYRBk9myaseg/related-artists'
    SEVERAL_ALBUMS = {
        'path': '/albums', 
        'params':{
            'ids':'382ObEPsp2rxGrnsizN5TX,1A2GTWGtFfWp7KSQTwWOyo,2noRn2Aes5aoNVsU6iWThc'
        }
    }
    GENRE_SEEDS = '/recommendations/available-genre-seeds'

    async with aiohttp.ClientSession() as session:
        scraper = Scraper(seed=[SEVERAL_ARTISTS], session=session)
        await scraper.run() 

if __name__ == "__main__":
    asyncio.run(main())