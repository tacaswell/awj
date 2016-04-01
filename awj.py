import os
import os.path
import time
import heapq

from random import shuffle
from glob import glob

import feather


class AWJ:
    '''LRU cache for DataFrames backed by on-disk feather files


    Parameters
    ----------
    cache_path : str or Path
        The location of the cache files

    max_size : float, optional
        The maximum size in MB of the cache directory.
    '''
    def __init__(self, cache_path, *, max_size=None):

        self._cache_path = cache_path
        self.max_size = max_size
        # convert to bytes
        if self.max_size is not None:
             self.max_size *= 1048576

        # TODO 2k compat
        os.makedirs(cache_path, exist_ok=True)
        self._fn_cache = dict()
        self._sz_cache = dict()
        # TODO replace this with a double linked list like boltons LRU
        self._heap_map = dict()
        self._heap = []

        # put files in to heap in random order
        files = glob(os.path.join(self._cache_path, '*feather'))
        shuffle(files)
        for fn in files:
            key = self._key_from_filename(fn)
            self._fn_cache[key] = fn
            stat = os.stat(fn)
            self._sz_cache[key] = stat.st_size
            heap_entry = [time.time(), key]
            heapq.heappush(self._heap, heap_entry)
            self._heap_map[key] = heap_entry

        # prune up front just in case
        self.__prune_files()

    def __prune_files(self):
        if self.max_size is None or not self.max_size > 0:
            return

        # TODO deal with pathological case of single file larger than max_size
        # as written this will result is all files being removed
        cur_size = sum(v for v in self._sz_cache.values())
        while cur_size > self.max_size:
            _, key = heapq.heappop(self._heap)
            if key in self:
                cur_size -= self._sz_cache[key]
                del self[key]

    def _filename_from_key(self, key):
        return os.path.join(self._cache_path, key + '.feather')

    def _key_from_filename(self, fn):
        fn, ext = os.path.splitext(os.path.basename(fn))
        return fn

    def __setitem__(self, key, df):
        fn = self._filename_from_key(key)
        feather.write_dataframe(df, fn)
        self._fn_cache[key] = fn
        self._sz_cache[key] = os.stat(fn).st_size
        if key in self._heap_map:
            self._heap_map[0] = time.time()
            # ensure the heap invariant
            heapq.heapify(self._heap)
        else:
            heap_entry = [time.time(), key]
            self._heap_map[key] = heap_entry
            heapq.heappush(self._heap, heap_entry)

        self.__prune_files()

    def __getitem__(self, key):
        fn = self._fn_cache[key]
        ret = feather.read_dataframe(fn)
        self._heap_map[0] = time.time()
        # ensure the heap invariant
        heapq.heapify(self._heap)
        return ret

    def __delitem__(self, key):
        fn = self._fn_cache.pop(key)
        self._sz_cache.pop(key)
        self._heap_map.pop(key)
        os.unlink(fn)

    def __contains__(self, key):
        return key in self._fn_cache

    def __iter__(self):
        return iter(self._fn_cache)
