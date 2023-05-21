from typing import List
import traceback
import argparse
import asyncio
import aiohttp
import redis.asyncio as redis
import spotify_client as sc
from cache import Cache
from artists_writer import ArtistsWriter
from backoff_policy import BackoffPolicy
from batch_artists_req_builder import BatchArtistsReqBuilder

NUM_WORKERS = 1
MAX_API_REQUESTS = 1

class Scraper:
    def __init__(self, seed: List[str], session: aiohttp.ClientSession):
        self._cache = Cache(fresh=FRESH)
        self._backoff_policy = BackoffPolicy(cap=10)
        self._artists_writer = ArtistsWriter(fresh=FRESH)
        self._session = session

        self._queue = asyncio.Queue()
        self._seed = seed
        self._total = 0
        self._api_requests = 0

        self._batch_artist_req_builder = BatchArtistsReqBuilder()


    async def run(self):
        await sc.initialize(self._session)
        for endpoint in self._seed:
            await self._queue.put(endpoint)

        print('[Scraper]: Starting Scraper...')
        print('[Scraper]: Initial Queue Size:', self._queue.qsize())
        workers = [asyncio.create_task(self.worker()) for _ in range(NUM_WORKERS)]

        await self._queue.join()
        print('[Scraper]: Queue has no unfinished tasks. Cancelling workers...')

        for w in workers:
            w.cancel()
        
        print('[Scraper]: finished.')

    async def worker(self):
        while True:
            try:
                await self.process_one()
            except asyncio.CancelledError:
                return
    
    async def process_one(self):
        endpoint = await self._queue.get()
        try:
            if self._api_requests < MAX_API_REQUESTS:
                self._api_requests += 1
                await self.process_endpoint(endpoint)
        except Exception as e:
            # TODO: retry handling i.e. requeue the endpoint in cases of
            # rate limit, connection error, etc.
            print(f'Error processing endpoint {endpoint}: {e}')
            traceback.print_exc()
        finally:
            self._queue.task_done()
    
    async def process_endpoint(self, endpoint):

        print('Processing:', endpoint)

        # cache check
        if await self._cache.exists(endpoint):
            print('Cache hit:', endpoint)
            return
        
        # rate limit
        wait_sec = await self._backoff_policy.get_backoff()
        if wait_sec > 0:
            print(f'Rate limit hit. Waiting {wait_sec} seconds...')
            await asyncio.sleep(wait_sec)
        
        # attempt to fetch
        headers = {
            'Authorization': f"Bearer {sc.ACCESS_TOKEN}"
        }
        res = await sc.fetch(session=self._session, url=sc.BASE+endpoint, method='GET', data=None, headers=headers)

        print(f'Endpoint: {endpoint} | Response: {None if res is None else res["status"]}')

        # handle response
        if res is None: # connection or other severe error
            # TODO: better handling? do we just requeue?
            await self._queue.put(endpoint)
        elif res['status'] == 429: # rate limit
            retry_after = res['data']['retry_after']
            self._backoff_policy.set_retry_after(retry_after)
            self._backoff_policy.incr_attempts()
            await self._queue.put(endpoint)
        elif res['status'] != 200: # other error handling
            # TODO: better handling? do we just requeue? this might be the case for expired tokens
            await self._queue.put(endpoint)
        else: # success
            await self.process_valid_endpoint(endpoint, res['data'])
            # add to cache
            # await self._cache.set(endpoint, b'1')
    
    async def process_valid_endpoint(self, endpoint, data):
        if endpoint.startswith(sc.STATIC_PATHS['genre_seeds']):
            print('Processing genre seeds...')
            await self.process_genres(data['genres'])
        elif endpoint.startswith(sc.STATIC_PATHS['recommendations']):
            print('Processing recommendations...')
            await self.process_artists([artist for track in data['tracks'] for artist in track["artists"]])
            await self.process_albums([album for track in data['tracks'] for album in track["album"]])
        elif endpoint.startswith(sc.STATIC_PATHS['artists']):
            print('Processing artists...')
            await self.process_artists(data['artists'])
        elif endpoint.startswith(sc.STATIC_PATHS['albums']):
            print('Processing albums...')
            await self.process_albums(data['albums'])
        elif endpoint.startswith(sc.STATIC_PATHS['artist_related_artists']):
            print('Processing related artists...')
            await self.process_artists(data['artists'])
    
    async def process_artists(self, artists):
        for artist in artists:
            artist_id = artist.get('id')
            genres = artist.get('genres')
            popularity = artist.get('popularity')
            name = artist.get('name')
            if artist_id is None or self._total >= MAX_NUM_ARTISTS:
                return
            print('processing artist:', artist_id)
            if await self._cache.exists(artist_id):
                continue

            # are we missing data about the artist?
            if genres is None or popularity is None or name is None:
                # print("Missing data for artist:", artist_id, "adding to batch request")
                await self._batch_artist_req_builder.add(artist_id)
                is_full = await self._batch_artist_req_builder.is_full()
                if is_full:
                    print("Batch request is full, enqueing batch request...")
                    ids = await self._batch_artist_req_builder.build()
                    await self._queue.put(f"/artists?ids={ids}")
            else:
                await self._artists_writer.add(id=artist_id, name=name, popularity=popularity, genres=genres)
                await self._cache.set(artist_id, b'1')
                self._total += 1
                # enqueue related artists
                await self._queue.put(f"/artists/{artist_id}/related-artists")
                # enqueue artist's genres
                await self.process_genres(genres)
    
    async def process_genres(self, genres):
        for genre in genres:
            if await self._cache.exists(genre):
                continue
            await self._queue.put(f"/recommendations?seed_genres={genre}")
            await self._cache.set(genre, b'1')
    
    async def process_albums(self, albums):
        pass
        




async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-n", "--max-num-artists", type=int, help="Set the max number of artists to scrape")
    parser.add_argument('-d', '--debug', action='store_true', help='Enable debug mode')
    parser.add_argument('-f', '--fresh', action='store_true', help='Start with a fresh cache and empty artists.csv')
    args = parser.parse_args()
    global DEBUG, MAX_NUM_ARTISTS, FRESH
    DEBUG = bool(args.debug)
    FRESH = bool(args.fresh)
    MAX_NUM_ARTISTS = args.max_num_artists or 100
    print(f"Debug mode: {DEBUG}")

    # Seeds for testing
    SEVERAL_ARTISTS = '/artists?ids=2CIMQHirSU0MQqyYHq0eOx,57dN52uHvrHOxijzpIgu3E,1vCWHaC5f2uS3yhpwWbIA6'
    RELATED_ARTISTS = '/artists/0TnOYISbd1XYRBk9myaseg/related-artists'


    async with aiohttp.ClientSession() as session:
        scraper = Scraper(seed=[RELATED_ARTISTS], session=session)
        await scraper.run() 

if __name__ == "__main__":
    asyncio.run(main())