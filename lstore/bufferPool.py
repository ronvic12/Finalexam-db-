from lstore.config import *
from collections import OrderedDict
from lstore.page import Page
from threading import Lock as Lock
import os
import glob


class BufferPool:

	_instance = None
	kwargs = {}

	# {"pageSize": 4096, "poolsize": 1028", "howManyToEvict": 100 }
	def __init__(self, **kwargs):
		raise Exception("singleton: Must call getBufferPool for instance")

	@classmethod
	def getBufferPool(cls):
		if cls._instance is None:
			cls._instance = cls.__new__(cls)
			cls._instance._init(**cls.kwargs)
		return cls._instance

	def _init(self, **kwargs):
		self._poolsize = kwargs.get("poolSize", BUFFER_POOL_SIZE)
		self._howManyToEvict = kwargs.get("howManyToEvict", HOW_MANY_TO_EVICT)
		self._path = kwargs.get("path", POOL_PATH_DEFAULT)
		self._pool = OrderedDict()
		self._merge_pool = OrderedDict()
		self._hitCount = 0
		self._missCount = 0
		self._pool_lock = Lock()
		if not os.path.exists(self._path):
			os.mkdir(self._path)

	def __len__(self):
		return len(self._pool) + len(self._merge_pool)

	def getPage(self, table, pageset, column) -> Page:
		pageKey = self.pageKeyEncoder(table, pageset, column)
		self._pool_lock.acquire()
		page = self._pool.get(pageKey)
		if page:
			# do if page is found in pool
			self._hitCount += 1
			self._pool.move_to_end(pageKey)
		else:
			self._missCount += 1
			while len(self) >= self._poolsize:
				self._evictLRU()
			page = self._readPageInStorage(pageKey)
			if page is None:
				page = self._newPage(pageKey)
			self._pool[pageKey] = page
		self._pool_lock.release()
		return page

	def updateBasePage(self, page_key, newPage: Page):
		old_page = self._pool.get(page_key)
		self._pool[page_key] = newPage
		self._merge_pool[page_key] = old_page

	def _newPage(self, pageKey) -> Page:

		while len(self) >= self._poolsize:
			self._evictLRU()
		newPage = self._pool.setdefault(pageKey,Page(pageKey))
		return newPage

	def haveAccessedPage(self, page):
		self._pool.move_to_end(page.pageKey)

	def getHitRate(self) -> float:
		return self._hitCount / (self._hitCount + self._missCount)

	def getMissRate(self) -> float:
		return self._missCount / (self._hitCount + self._missCount)

	def close(self):
		while len(self._pool) > 0:
			self._evict_last()
		while self._merge_pool:
			evicted_key, evict_page = self._merge_pool.popitem(False)
			if evict_page.dirty:
				self._writePageToStorage(evicted_key, evict_page)


	def _evictLRU(self):
		for _ in range(self._howManyToEvict):
			self._evict_last()

	def _writePageToStorage(self, pageKey, page: Page):
		d = page.data
		nextIndex = self.getNextIndexPage(pageKey)
		path = os.path.join(self._path, f'{pageKey}_{nextIndex}')
		with open(path, 'wb') as file:
			file.write(d)

	def _evict_last(self):
		evicted_key, evict_page = self._pool.popitem(False)
		if evict_page.dirty:
			self._writePageToStorage(evicted_key, evict_page)
		del evict_page

	def _readPageInStorage(self, pageKey):
		latest = self.getLatestFilePath(pageKey)
		path = os.path.join(self._path, latest)
		if not os.path.exists(path):
			return None
		with open(path, 'rb') as file:
			byte_array = bytearray(file.read(PAGE_ARRAY_SIZE))
			return Page(PAGE_CAPACITY, byte_array)

	@classmethod
	def resetBufferPoolCounter(cls):
		del cls._instance
		cls._instance = None

	def getresults(self):
		return f'hits: {self._hitCount}\n' \
		       f'miss: {self._missCount}\n' \
		       f'hit rate: {self.getHitRate()}\n' \
		       f'miss rate: {self.getMissRate()}\n'

	@staticmethod
	def pageKeyEncoder(table, pageset, column):
		return f'{table}_{pageset}_{column}'

	@staticmethod
	def _pageKeyDecoder(pageKey: str):
		return tuple(pageKey.split('_'))

	def getNextIndexPage(self, pageKey):
		return self.currentPageIndex(pageKey) + 1

	def currentPageIndex(self, pageKey):
		return max(self.getLikePageKeyIndexes(pageKey))

	def getLikePageKeyIndexes(self, pageKey):
		l = self.getLikePageKeyfilesPaths(pageKey)
		if l:
			return map((lambda x: int(x.removeprefix(pageKey))), l)
		else:
			return [-1]

	def getLatestFilePath(self, pageKey):
		l = self.getLikePageKeyfilesPaths(pageKey)
		if l:
			return max(l)
		else:
			return pageKey + f'_0'

	def getLikePageKeyfilesPaths(self, pageKey):
		return glob.glob1(self._path, pageKey + '_*')

