from typing import Dict, List
import asyncio

class ArtistsWriter:
    def __init__(self, fresh: bool):
        self.artists: Dict[str, List[str, int, List[str]]] = {}
        self._lock = asyncio.Lock()
        self._LIMIT = 100
        self._FILENAME = 'artists.csv'

        # prep file
        if fresh:
            self._write_header()
        else:
            try:
                with open(self._FILENAME, 'r') as _:
                    pass
            except FileNotFoundError:
                self._write_header()
    
    async def add(self, id: str, name: str, popularity: int, genres: List[str]):
        async with self._lock:
            self.artists[id] = [name, popularity, genres]
            if len(self.artists) >= self._LIMIT:
                await self._write_to_file()
                self.artists.clear()

    async def _write_to_file(self):
        with open(self._FILENAME, 'a') as f:
            items = self.artists.items()
            to_write = '\n'.join([f"{id},{name},{popularity},{';'.join(genres)}" for id, [name, popularity, genres] in items])
            f.write(to_write + "\n")
    
    def _write_header(self):
        with open(self._FILENAME, 'w') as f:
            f.write('id,name,popularity,genres\n')

