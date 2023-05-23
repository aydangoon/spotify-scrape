from spotify_client import STATIC_PATHS
import asyncio

class PathQueues:
    def __init__(self):
        self._queues = { path: [] for path in STATIC_PATHS.keys() }
        self._locks = { path: asyncio.Lock() for path in STATIC_PATHS.keys() }
        self._priority = [path for path in STATIC_PATHS.keys()]
        self._priority_lock = asyncio.Lock()
        self._size = 0
    
    async def put(self, path_key, item):
        async with self._locks[path_key]:
            self._queues[path_key].append(item)
            self._size += 1
    
    def empty(self):
        return self._size == 0

    async def set_priority(self, scores):
        async with self._priority_lock:
            self._priority = sorted(scores.keys(), key=lambda x: scores[x], reverse=True)
    
    async def flush(self, num=100):
        output = []
        for path in self._priority:
            if len(output) >= num:
                break
            async with self._locks[path]:
                size = min(num - len(output), len(self._queues[path]))
                output.extend(self._queues[path][:size])
                self._queues[path] = self._queues[path][size:]
        self._size -= len(output)
        return output
