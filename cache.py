import redis
import asyncio

class Cache:
    BATCHED = b'1'
    ADDED = b'2'

    def __init__(self, fresh=False):
        try:
            self.cache = redis.Redis(host='localhost', port=6379, db=0)
            self._lock = asyncio.Lock()
            if fresh:
                self.cache.flushall()
                print("[Cache]: cleared cache")
        except redis.exceptions.ConnectionError:
            print("[Cache]: Redis Server is not running on port 6379.")
            exit(1)

    async def get(self, key):
        async with self._lock:
            return self.cache.get(key)
    
    async def set(self, key, value):
        async with self._lock:
            return self.cache.set(key, value)
        
    async def exists(self, key):
        async with self._lock:
            return self.cache.exists(key) == 1

    async def close(self):
        async with self._lock:
            return self.cache.close()