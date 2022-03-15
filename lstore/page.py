from lstore.config import *
from threading import RLock as Lock

class Page:
    """
    :parameter num_records - int Number of record used in array
    :parameter bytes_used - bytes used for each entry
    :parameter capacity - Page entry capacity
    :parameter data - Page byte array of size array_size
    :Argument bytes_used - Overload bytes used per entry  Default 8 bytes
    :Argument array_size - Overload number of bytes in array Default 4096 bytes
    """
    def __init__(self, pageKey, setData: bytearray = None):
        self.data = setData if setData else bytearray(PAGE_ARRAY_SIZE)
        self.page_lock = Lock()
        self._rid_locks = {}
        self.dirty = False
        self.pageKey = pageKey #added this to identify the exact column that is being updated during merge
        self.flag = 0
        self.lineage = 0
        self.full = False
        self.merged = False

    def write(self, value, at_index):
        self.flag += 1
        self.dirty = True
        #self.data[at_index] = value
        start = at_index * NUM_BYTES_PER
        self.data[start:start+NUM_BYTES_PER] = value.to_bytes(NUM_BYTES_PER, 'big')

    # argument page_prefix as int value for pageset prefix
    # Will add RID to page in 8 byte for and return RID value
    # def writeRID(self, page_prefix, at_index) -> int:
    #     rid = rid_encoder(page_prefix, at_index)
    #     if not self.write(rid, at_index):
    #         return False
    #     return rid

    """
    input index: index value to retrieve from
    returns int value from bytearray
    """
    def read_at(self, index) -> int:
        self.flag += 1
        start = index * NUM_BYTES_PER
        return int.from_bytes(
            self.data[start:start+NUM_BYTES_PER], 'big')

    def getIndex(self):
        return self.pageKey

    def addLineage(self, value):
        self.lineage += value

    def releaseLock(self):
        self.flag -= 1
        if self.page_lock and self.flag == 0:
            self.page_lock.release()

    def getRidLockAt(self, index):
        lock = self._rid_locks.get(index)
        if not lock:
            lock = self._rid_locks.setdefault(index, Lock())
        return lock