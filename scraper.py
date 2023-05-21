import argparse
import asyncio
import redis.asyncio as redis
import spotify_client as sc
from cache import Cache
from artists_writer import ArtistsWriter

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

    # initialize
    await sc.initialize()
    cache = Cache()
    artists_writer = ArtistsWriter(fresh=FRESH)
    print(f"Debug mode: {DEBUG}")
    

if __name__ == "__main__":
    asyncio.run(main())