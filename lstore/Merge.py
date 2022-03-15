from lstore.config import *
from collections import OrderedDict
from lstore.page import Page
import os
from lstore.bufferPool import BufferPool
from queue import Queue
import copy
import time



class Merge:
    _instance = None

    def __init__(self):
        raise Exception("singleton: Must call getMerge for instance")

    @classmethod
    def getMerge(cls):
        if not cls._instance:
            cls._instance = cls.__new__(cls)
            cls._instance._init()
        return cls._instance

    def _init(self):
        self.queue = Queue()
        self.buffer = BufferPool.getBufferPool()
        self.waiting = {}
        self.up = True

    def shutDown(self):
        self.up = False


    def startMerge(self):
        while self.up:
            if self.queue:
                self.merge()
            time.sleep(1)

        self.cleanQueue()

    def addToQueue(self, is_full, containing_page_range, to_merge):
        if is_full:
            self.checkWaiting(containing_page_range)
            self.queue.put(to_merge)
        else:
            try:
                self.waiting[containing_page_range].append(to_merge)
            except:
                self.waiting[containing_page_range] = []
                self.waiting[containing_page_range].append(to_merge)

    def checkWaiting(self, page_range):
        if page_range not in self.waiting:
            return
        elements = self.waiting.pop(page_range)
        for elem in elements:
            self.queue.put(elem)

    def cleanQueue(self):
        print("Cleaning queue")
        keys=list(self.waiting.keys())
        for key in keys: # Merge everything before shut down
            elements = self.waiting.pop(key)
            for elem in elements:
                self.queue.put(elem)
        while self.queue.qsize() > 0:
            self.merge()

    def merge(self):
        print("merge is happening")

        currently_merged = 0
        table, tailpage_setnumber, update_column, bid_column, schema_column, rid_column, atIndex, containing_page_range = self.queue.get()

        table_name = table.getName()
        target_column = self.buffer.getPage(table_name, tailpage_setnumber, update_column)
        bid_column = self.buffer.getPage(table_name, tailpage_setnumber, BID_TID_COLUMN)
        schema_column = self.buffer.getPage(table_name, tailpage_setnumber, SCHEMA_ENCODING_COLUMN)
        rid_column = self.buffer.getPage(table_name, tailpage_setnumber, RID_COLUMN)

        basePages = []
        basePageCopies = {}
        basePageSchemas = {}
        updated_bids = []

        while currently_merged < MERGE_TRIGGER:
            schema = schema_column.read_at(atIndex)

            if schema == (schema | 1 << (update_column-FIRST_COLUMN)):
                currently_merged += 1
                bid = bid_column.read_at(atIndex)
                set_number, atIndex_base = rid_decoder(bid)

                if set_number not in basePages:
                    basePages.append(set_number)
                    copy_page = self.buffer.getPage(table_name, set_number, update_column)
                    basePageCopies[set_number] = [copy.deepcopy(copy_page), 0]
                    copy_page_schema = self.buffer.getPage(table_name, set_number, SCHEMA_ENCODING_COLUMN)
                    basePageSchemas[set_number] = copy_page_schema

                # add 1 to the lineage counter of that page
                basePageCopies[set_number][1] += 1

                if bid not in updated_bids:  # Found the latest update to this record!
                    updated_bids.append(bid)
                    # retrieve basepage copy
                    basePage = basePageCopies[set_number][0]
                    # obtain updated value
                    updatedvalue = target_column.read_at(atIndex)
                    # overwrite basepage copy
                    basePage.write(updatedvalue, atIndex_base)
                    # retrieve corresponding tid column
                    tid_col = self.buffer.getPage(table_name, set_number, BID_TID_COLUMN)
                    # obtain RID of latest update
                    latest_rid = rid_column.read_at(atIndex)
                    # write TID of basepage
                    tid_col.write(latest_rid, atIndex_base)
                    # retrieve corresponding schema column
                    schema_column_base = basePageSchemas[set_number]
                    # update schema column
                    schema_ = schema_column_base.read_at(atIndex_base)
                    schema_ = schema_ & 1 << (update_column-FIRST_COLUMN)
                    schema_column_base.write(schema_, atIndex_base)

            atIndex -= 1

            if atIndex == -1:
                tailpage_setnumber = containing_page_range.obtainPreviousTailPage(tailpage_setnumber)
                target_column = self.buffer.getPage(table_name, tailpage_setnumber, update_column)
                bid_column = self.buffer.getPage(table_name, tailpage_setnumber, BID_TID_COLUMN)
                schema_column = self.buffer.getPage(table_name, tailpage_setnumber, SCHEMA_ENCODING_COLUMN)
                rid_column = self.buffer.getPage(table_name, tailpage_setnumber, RID_COLUMN)
                atIndex = 511

        # UPDATE LINEAGE COUNTER
        for set_number in basePageCopies:
            baseColumn = basePageCopies[set_number][0]
            baseColumn.addLineage(basePageCopies[set_number][1])

            page_key = BufferPool.pageKeyEncoder(table_name, set_number, update_column)
            self.buffer.updateBasePage(page_key, baseColumn)

            tid_col = self.buffer.getPage(table_name, set_number, BID_TID_COLUMN)
            tid_col.addLineage(basePageCopies[set_number][1])

            #schema_col = basePageSchemas[set_number]
            #page_key_schemas = BufferPool.pageKeyEncoder(table_name, set_number, SCHEMA_ENCODING_COLUMN)
            #self.buffer.updateBasePage(page_key_schemas, schema_col)

def rid_decoder(rid: int):
    return rid >> BITS_USED_FOR_RID_INDEX, rid & BITS_USED_FOR_RID_MASK