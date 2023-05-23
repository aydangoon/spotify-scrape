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
from spotify_client import SpotifyClient, SpotifyAPIConstants

class Scraper:
    def __init__(self, seed: List[str], session: aiohttp.ClientSession):
        self.cache = Cache(fresh=FRESH)
        self.backoff_policy = BackoffPolicy()
        self._artists_writer = ArtistsWriter(fresh=FRESH)
        self.spotify_client = SpotifyClient(session)
        self.primary_queue = asyncio.Queue() # for batch artist requests, these take priority over other requests
        self.secondary_queue = asyncio.Queue()
        self.seed = seed
        self.total = 0
        self.artists_batch_builder = BatchReqBuilder(size=50)
        self.metrics = { # data collection
            key: { 
                'time': 0,
                'calls': 0,
                'batched': 0,
                'added': 0
            } for key in SpotifyAPIConstants.PATHS 
        }

    async def run(self):
        await self.spotify_client.refresh_access_token()
        for endpoint in self.seed:
            await self.primary_queue.put(endpoint)

        print('[Scraper]: Starting Scraper...')
        print('[Scraper]: Initial Queue Size:', self.secondary_queue.qsize())
        start = time.time()
        workers = [asyncio.create_task(self.worker()) for _ in range(NUM_WORKERS)]

        while not self.primary_queue.empty() or not self.secondary_queue.empty():
            await asyncio.sleep(5)
        print('[Scraper]: Queue has no unfinished tasks. Cancelling workers...')

        for w in workers:
            w.cancel()
        
        print(f'[Scraper]: finished in {time.time() - start} seconds')
        if DEBUG:
            print('[Scraper] Path Stats:')
            for key, val in self.metrics.items():
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
        processing_primary = not self.primary_queue.empty()
        if processing_primary:
            endpoint = await self.primary_queue.get()
        else:
            endpoint = await self.secondary_queue.get()
        try:
            if self.total < MAX_NUM_ARTISTS:
                await self.process_endpoint(endpoint)
        except Exception as e:
            print(f'Error processing endpoint {endpoint}: {e}')
            traceback.print_exc()
        finally:
            if processing_primary:
                self.primary_queue.task_done()
            else:
                self.secondary_queue.task_done()
    
    async def process_endpoint(self, endpoint):

        # attempt to fetch
        headers = {'Authorization': f"Bearer {self.spotify_client.access_token}"}
        start_time = time.time()
        res = await self.spotify_client.fetch(
            url=SpotifyAPIConstants.BASE+endpoint['path'],
            method='GET',
            data=None,
            headers=headers,
            params=endpoint['params']
        )
        call_time = time.time() - start_time

        if DEBUG:
            ep_str = endpoint['path'] + "| params: " + str(endpoint['params'])
            ep_str_abbrev = ep_str[:100]+"..." if len(ep_str) > 100 else ep_str
            debug(f'Response: {"XXX" if res is None else res["status"]} | Endpoint: {ep_str_abbrev}')

        # handle response
        if res is None: # connection or other severe error
            await self.secondary_queue.put(endpoint)
        elif res['status'] == SpotifyAPIConstants.RATE_LIMIT_CODE:
            debug('[Rate Limit]: warning')
            retry_after = res['data']['retry_after']
            await self.backoff_policy.set_retry_after(retry_after)
            await self.backoff_policy.incr_attempts()
            # worker waits rate limit before retrying
            wait_sec = await self.backoff_policy.get_backoff()
            if wait_sec > 0:
                debug(f'[Rate Limit]: Waiting {wait_sec} seconds...')
                await asyncio.sleep(wait_sec)
            await self.secondary_queue.put(endpoint)
        elif res['status'] == SpotifyAPIConstants.EXPIRED_TOKEN_CODE:
            debug("Refreshing access token...")
            await self.spotify_client.refresh_access_token()
            await self.secondary_queue.put(endpoint)
        elif res['status'] == SpotifyAPIConstants.BAD_OAUTH_CODE:
            debug('Bad OAuth token')
        else: # success
            await self.process_data(endpoint, res['data'], call_time)
    
    async def process_data(self, endpoint, data, call_time):
        path = endpoint['path']
        added, batched = 0, 0
        if path.startswith(SpotifyAPIConstants.GENRE_SEEDS_PATH):
            await self.process_genres(data['genres'])
            data_path = SpotifyAPIConstants.GENRE_SEEDS
        elif path.startswith(SpotifyAPIConstants.RECOMMENDATIONS_PATH):
            added, batched = await self.process_tracks(data['tracks'])
            data_path = SpotifyAPIConstants.RECOMMENDATIONS
        elif path.startswith(SpotifyAPIConstants.ARTIST_RELATED_ARTISTS_PATH):
            added, batched = await self.process_artists(data['artists'])
            data_path = SpotifyAPIConstants.ARTIST_RELATED_ARTISTS
        elif path.startswith(SpotifyAPIConstants.ARTISTS_PATH):
            added, batched = await self.process_artists(data['artists'])
            data_path = SpotifyAPIConstants.ARTISTS
        elif path.startswith(SpotifyAPIConstants.ALBUMS_PATH):
            added, batched = await self.process_albums(data['albums'])
            data_path = SpotifyAPIConstants.ALBUMS
        elif path.startswith(SpotifyAPIConstants.CATEGORY_PLAYLISTS_PATH):
            await self.process_category_playlists(data['playlists'])
            data_path = SpotifyAPIConstants.CATEGORY_PLAYLISTS
        elif path.startswith(SpotifyAPIConstants.CATEGORIES_PATH):
            await self.process_categories(data['categories'])
            data_path = SpotifyAPIConstants.CATEGORIES
        elif path.startswith(SpotifyAPIConstants.PLAYLIST_PATH):
            added, batched = await self.process_playlist(data)
            data_path = SpotifyAPIConstants.PLAYLIST
        elif path.startswith(SpotifyAPIConstants.SEARCH_PATH):
            added, batched = await self.process_search(data.get('artists', []))
            data_path = SpotifyAPIConstants.SEARCH

        self.metrics[data_path]['time'] += call_time
        self.metrics[data_path]['calls'] += 1
        self.metrics[data_path]['added'] += added
        self.metrics[data_path]['batched'] += batched
    
    async def process_tracks(self, tracks):
        artists = [artist for track in tracks for artist in track["artists"]]
        albums = [track['album'] for track in tracks]
        [added_by_artists, batched_by_artists] = await self.process_artists(artists)
        [added_by_albums, batched_by_albums] = await self.process_albums(albums)
        return [added_by_artists + added_by_albums, batched_by_artists + batched_by_albums]

    async def process_search(self, artists):
        [added, batched] = await self.process_artists(artists['items'])
        if artists.get('next', None) is not None:
            path = get_next_path(artists['next'])
            await self.secondary_queue.put({ 'path': path, 'params': None })
        return [added, batched]

    async def process_playlist(self, playlist):
        tracks = [item['track'] for item in playlist['items'] if item['track']['type'] == 'track']
        [added, batched] = await self.process_tracks(tracks)
        if playlist.get('next', None) is not None:
            path = get_next_path(playlist['next'])
            await self.secondary_queue.put({ 'path': path, 'params': None })
        return [added, batched]

    async def process_category_playlists(self, playlists):
        for playlist in playlists['items']:
            if playlist is None or await self.cache.exists(playlist['id']):
                continue
            await self.cache.set(playlist['id'], Cache.ADDED)
            await self.secondary_queue.put({ 
                'path': f"/playlists/{playlist['id']}/tracks", 
                'params': { 
                    'limit': 50,
                    'fields': 'next,items(track(type,album(id),artists(id)))'
                } 
            })
        if playlists.get('next', None) is not None:
            path = get_next_path(playlists['next'])
            await self.secondary_queue.put({ 'path': path, 'params': None })

    async def process_categories(self, categories):
        for category in categories['items']:
            if await self.cache.exists(category['id']):
                continue
            await self.cache.set(category['id'], Cache.ADDED)
            await self.secondary_queue.put({ 
                'path': f"/browse/categories/{category['id']}/playlists", 
                'params': { 'limit': 50 } 
            })
        if 'next' in categories and categories['next'] is not None:
            path = get_next_path(categories['next'])
            await self.secondary_queue.put({ 'path': path, 'params': None })

    async def process_genres(self, genres, artist_id=None):
        for genre in genres:
            if await self.cache.exists(genre):
                continue
            params = {'seed_genres': genre, 'limit': 100}
            if artist_id is not None:
                params['seed_artists'] = artist_id
            await self.secondary_queue.put({ 'path': '/recommendations', 'params': params })
            await self.primary_queue.put({ 'path': '/search', 'params': { 
                'q': f'genre:{genre}',
                'type': 'artist',
                'limit': 50 
            }})
            await self.cache.set(genre, Cache.ADDED)
    
    async def process_albums(self, albums):
        for album in albums:
            if await self.cache.exists(album['id']):
                continue
            await self.cache.set(album['id'], Cache.ADDED)
        return await self.process_artists([artist for album in albums for artist in album.get('artists', [])])

    async def process_artists(self, artists):
        debug(f'[PROCESSING]: {len(artists)}')
        added = 0
        batched = 0
        for artist in artists:
            if self.total >= MAX_NUM_ARTISTS: 
                break
            artist_id = artist.get('id')
            if artist_id is None:
                continue
            cache_val = await self.cache.get(artist_id)
            if cache_val == Cache.ADDED:
                continue

            genres, popularity, name = artist.get('genres'), artist.get('popularity'), artist.get('name')
            missing_data = genres is None or popularity is None or name is None
            if missing_data and cache_val == None:
                await self.cache.set(artist_id, Cache.BATCHED)
                await self.artists_batch_builder.add(artist_id)
                batched += 1
                if await self.artists_batch_builder.is_full():
                    await self.primary_queue.put({
                        'path': '/artists',
                        'params': {'ids': await self.artists_batch_builder.build()}
                    })
            elif not missing_data:
                await self._artists_writer.add(id=artist_id, name=name, popularity=popularity, genres=genres)
                added += 1 
                await self.cache.set(artist_id, Cache.ADDED)
                self.total += 1
                if self.total >= MAX_NUM_ARTISTS:
                    debug('Reached max number of artists. Stopping...')
                    break
                await self.secondary_queue.put({ 'path': f"/artists/{artist_id}/related-artists", 'params': None })
                await self.process_genres(genres, artist_id)
        debug(f'[ADDED]: {added} [BATCHED]: {batched}')
        return [added, batched]

def get_next_path(next_str: str):
    v1_idx = next_str.index('v1')
    return next_str[v1_idx+2:]

def debug(s: str):
    if DEBUG:
        print(s)
    
async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-n", "--max-num-artists", type=int, help="Set the max number of artists to scrape")
    parser.add_argument('-d', '--debug', action='store_true', help='Print debug statements')
    parser.add_argument('-f', '--fresh', action='store_true', help='Start with a fresh cache and empty artists.csv')
    parser.add_argument("-w", "--num-workers", type=int, help="Set the number of workers to use")
    args = parser.parse_args()
    global DEBUG, MAX_NUM_ARTISTS, FRESH, NUM_WORKERS
    DEBUG = bool(args.debug)
    FRESH = bool(args.fresh)
    MAX_NUM_ARTISTS = args.max_num_artists or 12_000_000 # spotify has ~11M artists as of 2023
    NUM_WORKERS = args.num_workers or 20 # best parallelism found while testing
    print(f"Debug mode: {DEBUG}")

    GENRE_SEEDS = { 'path': '/recommendations/available-genre-seeds', 'params': None }
    CATEGORY = { 'path': '/browse/categories', 'params': None}

    async with aiohttp.ClientSession() as session:
        scraper = Scraper(seed=[GENRE_SEEDS, CATEGORY], session=session)
        await scraper.run() 

if __name__ == "__main__":
    asyncio.run(main())