import asyncio

MAX_ARTISTS_PER_REQ = 50
class BatchArtistsReqBuilder:
    def __init__(self):
        self._artists = set()
        self._num_artists = 0
        self._lock = asyncio.Lock()
    
    async def add(self, artist_id):
        async with self._lock:
            if artist_id in self._artists:
                return
            self._artists.add(artist_id)
            self._num_artists += 1
    
    async def is_full(self):
        async with self._lock:
            return self._num_artists >= MAX_ARTISTS_PER_REQ
    
    async def build(self):
        async with self._lock:
            artists = self._artists
            self._artists = set()
            self._num_artists = 0
            return ",".join(artists)
