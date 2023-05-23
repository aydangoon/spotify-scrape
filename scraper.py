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
from spotify_client import SpotifyClient, BASE, PATHS, STATUS_CODES

RATE_LIMIIT_SAFETY = 5 # temp. if we get this number of 429s, just stop

class Scraper:
    def __init__(self, seed: List[str], session: aiohttp.ClientSession):
        self._cache = Cache(fresh=FRESH)
        self._backoff_policy = BackoffPolicy()
        self._artists_writer = ArtistsWriter(fresh=FRESH)
        self._sc = SpotifyClient(session)
        self._primary_queue = asyncio.Queue() # for batch artist requests, these take priority over other requests
        self._secondary_queue = asyncio.Queue()
        self._seed = seed
        self._total = 0
        self._rate_limit_hits = 0
        self._batch_artist_req_builder = BatchReqBuilder(size=50)
        self._path_data = { key: { 'time': 0, 'calls': 0, 'batched': 0, 'added': 0 }for key in PATHS.keys() } # data collection

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
        if DEBUG:
            print('[Scraper] Path Stats:')
            for key, val in self._path_data.items():
                total_time, calls, batched, added = val['time'], val['calls'], val['batched'], val['added']
                print("======================================================")
                print(key+":")
                time_per_call = 0 if calls == 0 else total_time/calls
                info_per_call = 0 if calls == 0 else (added + 0.5*batched)/calls
                info_per_sec = 0 if calls == 0 else info_per_call/time_per_call
                print(f"\tTotal Time: {total_time}")
                print(f"\tTime per call: {time_per_call}")
                print(f"\tInfo per call: {info_per_call}")
                print(f"\tInfo per second: {info_per_sec}")
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

        if self._rate_limit_hits >= RATE_LIMIIT_SAFETY:
            debug('[Rate Limit]: safety hit. Stopping...')
            return
        
        # rate limit
        wait_sec = await self._backoff_policy.get_backoff()
        if wait_sec > 0:
            debug(f'[Rate Limit]: Waiting {wait_sec} seconds...')
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

        if DEBUG:
            ep_str = endpoint['path'] + "| params: " + str(endpoint['params'])
            ep_sr_abbrev = ep_str[:100]+"..." if len(ep_str) > 100 else ep_str
            debug(f'Response: {"XXX" if res is None else res["status"]} | Endpoint: {ep_sr_abbrev}')

        # handle response
        if res is None: # connection or other severe error
            await self._secondary_queue.put(endpoint)
        elif res['status'] == STATUS_CODES['RATE_LIMITED']:
            debug('[Rate Limit]: warning')
            self._rate_limit_hits += 1
            retry_after = res['data']['retry_after']
            await self._backoff_policy.set_retry_after(retry_after)
            await self._backoff_policy.incr_attempts()
            await self._secondary_queue.put(endpoint)
        elif res['status'] == STATUS_CODES['OLD_ACCESS_TOKEN']:
            debug("Refreshing access token...")
            await self._sc.refresh_access_token()
            await self._secondary_queue.put(endpoint)
        elif res['status'] == STATUS_CODES['BAD_OAUTH']:
            debug('Bad OAuth token')
        else: # success
            await self.process_data(endpoint, res['data'], call_time)
    
    async def process_data(self, endpoint, data, call_time):
        path = endpoint['path']
        [added, batched] = [0, 0]
        if path.startswith(PATHS['genre_seeds']):
            await self.process_genres(data['genres'])
            path = 'genre_seeds'
        elif path.startswith(PATHS['recommendations']):
            [a0, b0] = await self.process_artists([artist for track in data['tracks'] for artist in track["artists"]])
            [a1, b1] = await self.process_albums([track['album'] for track in data['tracks']])
            added += a0 + a1
            batched += b0 + b1
            path = 'recommendations'
        elif path.startswith(PATHS['artist_related_artists']):
            [a0, b0] = await self.process_artists(data['artists'])
            added += a0
            batched += b0
            path = 'artist_related_artists'
        elif path.startswith(PATHS['artists']):
            [a0, b0] = await self.process_artists(data['artists'])
            added += a0
            batched += b0
            path = 'artists'
        elif path.startswith(PATHS['albums']):
            [a0, b0] = await self.process_albums(data['albums'])
            added += a0
            batched += b0
            path = 'albums'
        elif path.startswith(PATHS['category_playlists']):
            await self.process_category_playlists(data['playlists'])
            path = 'category_playlists'
        elif path.startswith(PATHS['categories']):
            await self.process_categories(data['categories'])
            path = 'categories'
        elif path.startswith(PATHS['playlist']):
            [a0, b0] = await self.process_playlist(data)
            added += a0
            batched += b0
            path = 'playlist'
        elif path.startswith(PATHS['search']):
            [a0, b0] = await self.process_search(data.get('artists', []))
            added += a0
            batched += b0
            path = 'search'

        self._path_data[path]['time'] += call_time
        self._path_data[path]['calls'] += 1
        self._path_data[path]['added'] += added
        self._path_data[path]['batched'] += batched
    
    async def process_search(self, artists):
        [a0, b0] = await self.process_artists(artists['items'])
        if artists.get('next', None) is not None:
            next_str = artists['next']
            v1_idx = next_str.index('v1')
            path = next_str[v1_idx+2:]
            await self._secondary_queue.put({ 'path': path, 'params': None })
        return [a0, b0]

    async def process_playlist(self, playlist):
        tracks = [item['track'] for item in playlist['items'] if item['track']['type'] == 'track']
        [a0, b0] = await self.process_artists([artist for track in tracks for artist in track['artists']])
        [a1, b1] = await self.process_albums([track['album'] for track in tracks])
        if playlist.get('next', None) is not None:
            next_str = playlist['next']
            v1_idx = next_str.index('v1')
            path = next_str[v1_idx+2:]
            await self._secondary_queue.put({ 'path': path, 'params': None })
        return [a0 + a1, b0 + b1]

    async def process_category_playlists(self, playlists):
        for playlist in playlists['items']:
            if playlist is None or await self._cache.exists(playlist['id']):
                continue
            await self._cache.set(playlist['id'], b'1')
            await self._secondary_queue.put({ 
                'path': f"/playlists/{playlist['id']}/tracks", 
                'params': { 
                    'limit': 50,
                    'fields': 'next,items(track(type,album(id),artists(id)))'
                } 
            })
        if playlists.get('next', None) is not None:
            next_str = playlists['next']
            v1_idx = next_str.index('v1')
            path = next_str[v1_idx+2:]
            await self._secondary_queue.put({ 'path': path, 'params': None })

    async def process_categories(self, categories):
        for category in categories['items']:
            if await self._cache.exists(category['id']):
                continue
            await self._cache.set(category['id'], b'1')
            await self._secondary_queue.put({ 
                'path': f"/browse/categories/{category['id']}/playlists", 
                'params': { 'limit': 50 } 
            })
        if 'next' in categories and categories['next'] is not None:
            next_str = categories['next']
            v1_idx = next_str.index('v1')
            path = next_str[v1_idx+2:]
            await self._secondary_queue.put({ 'path': path, 'params': None })

    async def process_genres(self, genres, artist_id=None):
        for genre in genres:
            if await self._cache.exists(genre):
                continue
            params = {'seed_genres': genre, 'limit': 100}
            if artist_id is not None:
                params['seed_artists'] = artist_id
            await self._secondary_queue.put({ 'path': '/recommendations', 'params': params })
            await self._secondary_queue.put({ 'path': '/search', 'params': { 
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
        debug(f'[PROCESSING]: {len(artists)}')
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
                if await self._batch_artist_req_builder.is_full():
                    await self._primary_queue.put({
                        'path': '/artists',
                        'params': {'ids': await self._batch_artist_req_builder.build()}
                    })
            else:
                await self._artists_writer.add(id=artist_id, name=name, popularity=popularity, genres=genres)
                added_artists += 1 
                await self._cache.set(artist_id, b'2')
                self._total += 1
                if self._total >= MAX_NUM_ARTISTS:
                    debug('Reached max number of artists. Stopping...')
                    break
                await self._secondary_queue.put({ 'path': f"/artists/{artist_id}/related-artists", 'params': None })
                await self.process_genres(genres, artist_id)
        debug(f'[ADDED]: {added_artists} [BATCHED]: {batched_artists}')
        return [added_artists, batched_artists]

def debug(s: str):
    if DEBUG:
        print(s)
    
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
    MAX_NUM_ARTISTS = args.max_num_artists or 11_000_000 # spotify has ~11M artists as of 2023
    NUM_WORKERS = args.num_workers or 20 # best parallelism found while testing
    print(f"Debug mode: {DEBUG}")

    # various seeds for testing
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