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

RATE_LIMIIT_SAFETY = 1 # temp. if we get this number of 429s, just stop

class Scraper:
    def __init__(self, seed: List[str], session: aiohttp.ClientSession):
        self._cache = Cache(fresh=FRESH)
        self._backoff_policy = BackoffPolicy()
        self._artists_writer = ArtistsWriter(fresh=FRESH)
        self._sc = SpotifyClient(session)

        self._queue = asyncio.Queue()
        self._seed = seed
        self._total = 0
        self._rate_limit_hits = 0

        self._batch_artist_req_builder = BatchReqBuilder(size=50)


    async def run(self):
        await self._sc.refresh_access_token()
        for endpoint in self._seed:
            await self._queue.put(endpoint)

        print('[Scraper]: Starting Scraper...')
        print('[Scraper]: Initial Queue Size:', self._queue.qsize())
        start = time.time()
        workers = [asyncio.create_task(self.worker()) for _ in range(NUM_WORKERS)]

        await self._queue.join()
        print('[Scraper]: Queue has no unfinished tasks. Cancelling workers...')

        for w in workers:
            w.cancel()
        
        print(f'[Scraper]: finished in {time.time() - start} seconds')

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
            # TODO: retry handling i.e. requeue the endpoint in cases of
            # rate limit, connection error, etc.
            print(f'Error processing endpoint {endpoint}: {e}')
            traceback.print_exc()
        finally:
            self._queue.task_done()
    
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
        res = await self._sc.fetch(url=BASE+endpoint, method='GET', data=None, headers=headers)

        print(f'Response: {"XXX" if res is None else res["status"]} | Endpoint: {endpoint}')

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
            await self.process_valid_endpoint(endpoint, res['data'])
    
    async def process_valid_endpoint(self, endpoint, data):
        if endpoint.startswith(STATIC_PATHS['genre_seeds']):
            #print('Processing genre seeds...')
            await self.process_genres(data['genres'])
        elif endpoint.startswith(STATIC_PATHS['recommendations']):
            #print('Processing recommendations...')
            await self.process_artists([artist for track in data['tracks'] for artist in track["artists"]])
            await self.process_albums([track['album'] for track in data['tracks']])
        elif endpoint.startswith(STATIC_PATHS['artists']):
            #print('Processing artists...')
            await self.process_artists(data['artists'])
        elif endpoint.startswith(STATIC_PATHS['albums']):
            #print('Processing albums...')
            await self.process_albums(data['albums'])
        elif endpoint.startswith(STATIC_PATHS['artist_related_artists']):
            #print('Processing related artists...')
            await self.process_artists(data['artists'])
        # TODO: categories and playlists

    async def process_genres(self, genres):
        for genre in genres:
            if await self._cache.exists(genre):
                continue
            await self._queue.put(f"/recommendations?seed_genres={genre}")
            await self._cache.set(genre, b'1')
    
    async def process_albums(self, albums):
        for album in albums:
            if await self._cache.exists(album['id']):
                continue
            if 'artists' in album:
                await self.process_artists(album['artists'])
                await self._cache.set(album['id'], b'1')
    
    async def process_artists(self, artists):
        print(f'processing {len(artists)} artists')
        for artist in artists:
            artist_id = artist.get('id')
            if artist_id is None: 
                return
            if self._total >= MAX_NUM_ARTISTS:
                print('Reached max number of artists. Stopping...')
                return

            print('processing artist:', artist_id)

            genres, popularity, name = artist.get('genres'), artist.get('popularity'), artist.get('name')
            cache_val = await self._cache.get(artist_id)
            # are we missing data about the artist?
            if cache_val is None and (genres is None or popularity is None or name is None):
                await self._cache.set(artist_id, 1)
                await self._batch_artist_req_builder.add(artist_id)
                if await self._batch_artist_req_builder.is_full():
                    print("Batch request is full, enqueing batch request...")
                    ids = await self._batch_artist_req_builder.build()
                    await self._queue.put(f"/artists?ids={ids}")
            elif cache_val is None or cache_val == 1:
                await self._artists_writer.add(id=artist_id, name=name, popularity=popularity, genres=genres)
                await self._cache.set(artist_id, 2)
                self._total += 1
                # enqueue related artists
                await self._queue.put(f"/artists/{artist_id}/related-artists")
                # enqueue artist's genres
                await self.process_genres(genres)
    
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
    SEVERAL_ARTISTS = '/artists?ids=2CIMQHirSU0MQqyYHq0eOx,57dN52uHvrHOxijzpIgu3E,1vCWHaC5f2uS3yhpwWbIA6'
    RELATED_ARTISTS = '/artists/0TnOYISbd1XYRBk9myaseg/related-artists'
    SEVERAL_ALBUMS = '/albums?ids=382ObEPsp2rxGrnsizN5TX,1A2GTWGtFfWp7KSQTwWOyo,2noRn2Aes5aoNVsU6iWThc'
    GENRE_SEEDS = '/recommendations/available-genre-seeds'

    async with aiohttp.ClientSession() as session:
        scraper = Scraper(seed=[SEVERAL_ARTISTS], session=session)
        await scraper.run() 

if __name__ == "__main__":
    asyncio.run(main())