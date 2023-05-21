import argparse
import asyncio
import spotify_client as sc


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-n", "--max-num-artists", type=int, help="Set the max number of artists to scrape")
    parser.add_argument('-d', '--debug', action='store_true', help='Enable debug mode')
    parser.add_argument('-cc', '--clear-cache', action='store_true', help='Start with a fresh cache')
    args = parser.parse_args()
    global DEBUG, MAX_NUM_ARTISTS, CLEAR_CACHE
    DEBUG = bool(args.debug)
    CLEAR_CACHE = bool(args.clear_cache)
    MAX_NUM_ARTISTS = args.max_num_artists or 100
    await sc.initialize()
    

if __name__ == "__main__":
    asyncio.run(main())