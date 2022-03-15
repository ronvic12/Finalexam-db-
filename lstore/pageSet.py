import threading
from time import time_ns
from lstore.bufferPool import BufferPool
from lstore.config import *
from threading import RLock as Lock
from lstore.threadManager import TransactionThread, current_thread
from lstore.lockManager import LockManager as lm
from lstore.abortException import ABORTEXCEPTION



class PageSet:

    def __init__(self, num_columns, table, activeRange=None, set_number=None):
        self.num_records = PAGE_CAPACITY if set_number else 0
        self.counterLock = Lock()
        self.set_number = set_number if set_number else table.index.get_new_set_number()
        self.activeRange = activeRange
        self.bufferpool = BufferPool.getBufferPool()
        self.table = table
        self.pageset_lock = Lock()

    def has_capacity(self):
        return self.num_records < PAGE_CAPACITY

    def attachToRange(self, range_to_attach):
        self.activeRange = range_to_attach

    def addNewRecord(self, command, bid=None, indirection=None, schema=None):
        if not self.has_capacity():
            return False
        at_index = self._get_next_index()
        if indirection:
            self._getLockandWriteToPage(INDIRECTION_COLUMN, at_index, indirection)
        self._getLockandWriteToPage(TIMESTAMP_COLUMN, at_index, time_ns())
        if schema:
            self._getLockandWriteToPage(SCHEMA_ENCODING_COLUMN, at_index, schema)
        if bid:
            self._getLockandWriteToPage(BID_TID_COLUMN, at_index, bid)
        for i, v in enumerate(command):
            self._getLockandWriteToPage(FIRST_COLUMN + i, at_index, v)
        rid = self._writeRIDToPage(RID_COLUMN, at_index)
        return rid

    def _get_next_index(self) -> int:
        self.counterLock.acquire()
        at_index = self.num_records
        self.num_records += 1
        self.counterLock.release()
        return at_index


    def readPageAtIndex(self, page_index, atIndex):

        return self._getLockandReadFromPage(page_index, atIndex)

    def updateSchema(self, new_schema, at_Index):
        page = self.getPage(SCHEMA_ENCODING_COLUMN)
        schema = page.read_at(at_Index)
        schema = new_schema | schema
        self._getLockandWriteToPage(SCHEMA_ENCODING_COLUMN, at_Index, schema)
        #page.write(schema, at_Index)

    def updateIndirection(self, atIndex, rid_tail):
        self._getLockandWriteToPage(INDIRECTION_COLUMN, atIndex, rid_tail)

    def getPage(self, page_Index):
        return self.bufferpool.getPage(self.table.name, self.set_number, page_Index)

    def changePagePointer(self, pageKey, newPage):
        self.bufferpool.updateBasePage(pageKey, newPage)

    def returnPageRange(self):
        return self.activeRange

    def _getLockandWriteToPage(self, page_Index, at_Index, data):
        lock = lm.getRidLock(rid_encoder(self.set_number, at_Index), exclusive=True)
        current_thread().addAction(target=self._WriteToPage, arguments=(page_Index, at_Index, data), lock=lock)

    def _WriteToPage(self, page_Index, at_Index, data):
        page = self.bufferpool.getPage(self.table.name, self.set_number, page_Index)
        page.write(data, at_Index)

    def _getLockandReadFromPage(self, page_index, at_Index):
        page = self.getPage(page_index)
        lock = lm.getRidLock(rid_encoder(self.set_number, at_Index), exclusive=False)
        value = page.read_at(at_Index)
        lock.release()
        return value

    def _writeRIDToPage(self, pageset_id, at_Index):
        rid = rid_encoder(self.set_number, at_Index)
        self._getLockandWriteToPage(pageset_id, at_Index, rid)
        return rid

    def getSetNumber(self):
        return self.set_number
