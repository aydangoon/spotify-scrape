from typing import List
import argparse
import asyncio
import aiohttp
import redis.asyncio as redis
import spotify_client as sc
from cache import Cache
from artists_writer import ArtistsWriter
from backoff_policy import BackoffPolicy

NUM_WORKERS = 1

class Scraper:
    def __init__(self, seed: List[str], session: aiohttp.ClientSession):
        self._cache = Cache(fresh=FRESH)
        self._backoff_policy = BackoffPolicy(cap=10)
        self._artists_writer = ArtistsWriter(fresh=FRESH)
        self._session = session

        self._queue = asyncio.Queue()
        self._seed = seed
        self._total = 0
        self._worker_id = 0


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
            await self.process_endpoint(endpoint)
        except Exception as e:
            # TODO: retry handling i.e. requeue the endpoint in cases of
            # rate limit, connection error, etc.
            print(f'Error processing endpoint {endpoint}: {e.with_traceback().print_stack()}')
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
            await self._cache.set(endpoint, b'1')
    
    async def process_valid_endpoint(self, endpoint, data):
        if endpoint.startswith('/artists/'):
            await self._artists_writer.add(
                id=data['id'],
                name=data['name'],
                genres=data['genres'],
                popularity=data['popularity']
            )




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

    async with aiohttp.ClientSession() as session:
        scraper = Scraper(seed=['/artists/1oPRcJUkloHaRLYx0olBLJ'], session=session)
        await scraper.run() 

if __name__ == "__main__":
    asyncio.run(main(), debug=True)