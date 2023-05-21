import asyncio

class BatchReqBuilder:
    def __init__(self, size):
        self._ids = set()
        self._num_ids = 0
        self._size = size
        self._lock = asyncio.Lock()
    
    async def add(self, elt_id):
        async with self._lock:
            if elt_id in self._ids:
                print('duplicate id:', elt_id)
                return
            self._ids.add(elt_id)
            self._num_ids += 1
    
    async def is_full(self):
        async with self._lock:
            return self._num_ids >= self._size
    
    async def build(self):
        async with self._lock:
            ids = self._ids
            self._ids = set()
            self._num_ids = 0
            return ",".join(ids)
