import asyncio
from random import uniform

class BackoffPolicy:
    def __init__(self):
        self._cap = 1800 # 30 minutes
        self._base = 1
        self._attempts = 0 
        self._lock = asyncio.Lock()
        self._retry_after = None
    
    async def incr_attempts(self):
        async with self._lock:
            self._attempts += 1
    
    async def set_retry_after(self, retry_after):
        async with self._lock:
            if self._retry_after is None or retry_after < self._retry_after:
                self._retry_after = retry_after
    
    # min of server suggested "Retry After" header and our calculated full jitter
    async def get_backoff(self):
        async with self._lock:
            if self._attempts == 0:
                return 0
            jitter = uniform(0, min(self._cap, self._base * 2 ** (self._attempts-1)))
            if self._retry_after is not None:
                return min(self._retry_after, jitter)
            return jitter