# Spotify API Artist Scraper

author: Aydan Gooneratne
email: aydang

## Usage

To use `spotify-scraper` first install dependencies with:

```pip3 install -r requirements.txt```

Next run a redis server on port 6379. Redis setup instructions can be found [here](https://redis.io/topics/quickstart).
If you already have redis install locally this can be done with:

```redis-server```

Finally run the main script with:

```python3 ./scraper.py```

The script takes a few command line options:
* `-n <num>`: number of artists to scrape, if not present will scrape all artists
* `-f`: include this flag if you want a fresh scrape (i.e. delete all cached data beforehand)
* `-d`: include this flag to see debug output.
* `-w <num>`: number of worker coroutines to use, defaults to 10 for small loads, 20 for larger loads. 

The script will store artists in a csv file `artists.csv` in the current directory. `artists.csv` contains columns
`id`, `name`, `popularity`, and `genres`. `genres` is a semicolon separated list of genres.

## Approach

### Backoff Strategy

### Asynchronous Requests

### Caching

### Crawling Strategy
