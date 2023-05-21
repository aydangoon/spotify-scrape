import redis
import asyncio

class Cache:
    def __init__(self, fresh=False):
        try:
            self.cache = redis.Redis(host='localhost', port=6379, db=0)
            self._lock = asyncio.Lock()
            if fresh:
                self.cache.flushall()
        except redis.exceptions.ConnectionError:
            print("[Cache]: Redis Server is not running on port 6379.")
            exit(1)

    async def get(self, key):
        async with self._lock:
            return self.cache.get(key)
    
    async def set(self, key, value):
        async with self._lock:
            print("[Cache]: Setting key:", key)
            return self.cache.set(key, value)
        
    async def exists(self, key):
        async with self._lock:
            return self.cache.exists(key)

    async def close(self):
        async with self._lock:
            return self.cache.close()