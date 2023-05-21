import redis
import asyncio

class Cache:
    def __init__(self):
        try:
            self.cache = redis.Redis(host='localhost', port=6379, db=0)
        except redis.exceptions.ConnectionError:
            print("Redis Server is not running on port 6379.")
            exit(1)
        self._lock = asyncio.Lock()

    async def get(self, key):
        async with self._lock:
            return self.cache.get(key)
    
    async def set(self, key, value):
        async with self._lock:
            return self.cache.set(key, value)
        
    async def exists(self, key):
        async with self._lock:
            return self.cache.exists(key)

    async def close(self):
        async with self._lock:
            return self.cache.close()