# Spotify API Artist Scraper

## Usage

First clone repo and cd into it:

```git clone https://github.com/aydangoon/spotify-scrape.git && cd spotify-scrape```

Install dependencies with:

```pip3 install -r requirements.txt```

[Create a Spotify App](https://developer.spotify.com/dashboard/create), and then in the current directory make a file `key.json` with contents:

```
{
    "client_id": "<your client id>",
    "client_secret": "<your client secret>"
}
```

ClientID and ClientSecret can be found via your Spotify App dashboard -> Settings -> Client ID and Client Secret.

Next in a separate terminal or in the background, run a redis server on port 6379 ([Redis setup](https://redis.io/docs/getting-started/installation/)). On Mac this is:

```brew install redis && redis-server &```

Finally run the main script with:

```python3 ./scraper.py```

The script takes a few command line options:
* `-n <num>`: number of artists to scrape, if not present will scrape all artists.
* `-f`: include this flag if you want a fresh scrape (i.e. delete all cached data beforehand)
* `-d`: include this flag to see debug output.
* `-w <num>`: number of worker coroutines to use, defaults to 20.

The script will store artists in a csv file `artists.csv` in the current directory. `artists.csv` contains columns
`id`, `name`, `popularity`, and `genres`. `genres` is a semicolon separated list of genres.

## Approach
### Overview
I approached this problem similar to a multi-threaded web crawler. The main architecture is a coroutine pool of workers
and a queue of API endpoints. Workers get endpoints from the queue, make requests to the API, store any artists found,
push any good* endpoints back onto the queue, and repeat. The script ends when the queue is empty or the artist target is met.
The queue is started with two endpoints that contain all genres and categories.

### Backoff Strategy
To word around the API rate limit, I implemented a backoff strategy based on [AWS Expontial Backoff with Jitter](https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/). Essentially if a request hits the rate limit, the worker will wait ~2^attempts seconds with some random jitter before retrying. If the server provides a shorter `Retry-After` header, the worker will wait that amount of time instead. This allows the worker to exponentially backoff until it complies with the rate limit.

### Asynchronous Requests
I used `aiohttp` and `asyncio` to make asynchronous requests. This allowed workers to process requests in parallel and not block on network IO.

### Caching
I used `redis` to cache [Spotify IDs](https://developer.spotify.com/documentation/web-api/concepts/spotify-uris-ids) of resources that had already been scraped. This was to stop duplicate requests from being added to the task queue, and worse, from being sent to the API.

### Crawling Strategy
I used a breadth-first search strategy with a couple tweaks. Before even writing any code, I explored the docs to find endpoints that
returned artist data, and endpoints that pointed to those endpoints. For example, some were:
* `GET /artists?ids={ids}`: returns artists
* `GET /artists/{id}/related-artists`: returns related artists
* `GET /albums?ids={ids}`: return albums (which contain artists)
* `GET /browse/categories`: returns categories (which contain category playlists)
* `GET /playlists/{id}/tracks`: return tracks (which contain artists)
* `GET /search`: return artists, albums, playlists, and tracks

The strategy was breadth-first in the sense that once an endpoint was scraped, it would be marked as "seen" (Spotify ID cached),
and any endpoints found in the response would be enqueued. The main difference from traditional BFS was that I included a 
primary queue and secondary queue. The primary queue was for endpoints that returned a high amount of "information" per call,
but lower breadth. The secondary queue was for endpoints that returned a lower amount of "information" per call but have higher breadth. Information is:

```info = artists_added + 0.5*artists_batched```

Where `artists_added` is the number of artists added to the database in the single call and `artists_batched` is the number of artists for which we got incomplete data in the call, so their ids are added to a batch request to be sent later.

Since the API has a artist batch request endpoint, I always send batch requests for incomplete artists rather than individual
requests per artists as this reduces the number of requests sent to the API. Additionally, since I always send high information
per call requests first, the number of API calls is minimized.

### Testing
I collected metrics on information/call, time/call and number of calls for each endpoint. I used this to tweak the crawling strategy
and prioritize some endpoints over others. Additionally I read the docs for each endpoint to maximize # of items returned per call 
and remove unnecessary fields when possible. I also tested different numbers of workers. For my machine, 20 workers was best,
but this may vary depending on cores and network speed which is why I included a command line option for number of workers.

I think next steps would be to replace the queue with a priority queue that prioritized endpoints with high information/call and
update the information/call for each endpoint type dynamically as the program ran.