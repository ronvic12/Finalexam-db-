from lstore.config import *
from lstore.index import Index
from lstore.pageRange import PageRange
from lstore.pageSet import PageSet
from queue import Queue
import copy
from lstore.Merge import Merge
from lstore.threadManager import current_thread
from threading import RLock as Lock


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

    def __str__(self):
        return f'{self.columns}'

class Table:
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def __init__(self, name, num_columns, key, index=None):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.index = Index(self, index)
        self.active_pageRange = PageRange(num_columns, table=self, create_empty_set=True)
        self.page_ranges = {self.active_pageRange}
        self.merge = Merge.getMerge()
        self.page_directory_lock = Lock()

    def get_new_pageset_num(self):
        return self.index.get_new_set_number()

    def addToPageDirectory(self, pageSet):
        self.page_directory[pageSet.set_number] = pageSet

    def add_record(self, command):
        while True:
            result = self.active_pageRange.addNewRecord(command)
            # if pageRange is full create new page_range
            if result:
                set_num, _ = rid_decoder(result[0])
                # add add to pagedirecteory page set pointer
                self.page_directory[set_num] = result[1]
                # add to index
                for i, c in enumerate(command):
                    current_thread().addAction(target=self.index.add_to_index, arguments=(i, c, result[0]))
                    #DO ADD ACTION FOR INDEX INSER
                    #self.index.add_to_index(column=i, key_value=c, rid=result[0])
                return True
            else:
                self.active_pageRange.setFull()
                self.active_pageRange = PageRange(num_columns=self.num_columns, table=self, create_empty_set=True)
                self.page_ranges.add(self.active_pageRange)

    def update_record(self, primary_key, commands):
        if commands[self.key]:
            return False
        rid_base = self.index.locate(self.key, primary_key)
        # rid not found
        if not rid_base:
            return False
        rid_base = rid_base[0]
        set_number, atIndex = rid_decoder(rid_base)
        #base_set = self.page_directory.get(set_number)
        base_set = self._getFromDirectory(set_number)
        if not base_set:
            base_set = PageSet(self.num_columns, self, set_number=set_number)
            self.addToPageDirectory(base_set)
        indirection = base_set.readPageAtIndex(INDIRECTION_COLUMN, atIndex)
        if not indirection:
            indirection = rid_base
        containing_page_range = base_set.returnPageRange()
        if not containing_page_range:
            self.active_pageRange.addSet(base_set)
            containing_page_range = base_set.activeRange

        check = [None] * self.num_columns
        need_merge = 0
        for i, c in enumerate(commands):
            if c:
                has_index = self.index.has_index(i)
                if has_index:
                    check[i] = self.index.has_index(
                        i)  # check will contain a True only if the updated column has an index
        sel = all(v is None for v in check)
        if not sel:
            prev_value = self.select([rid_base], check)
        rid_tail, schema = containing_page_range.addToTail(bid=rid_base, indirection=indirection, commands=commands,
                                                           baseSet=base_set)
        base_set.updateIndirection(atIndex, rid_tail)
        base_set.updateSchema(schema, atIndex)
        if not sel:

            for i, command in enumerate(commands):
                if command:
                    current_thread().addAction(target=self.index.update, arguments=(i, rid_base, prev_value[0].columns[i], command))
                    #self.index.update(i, rid_base, prev_value[0].columns[i], command)

        if need_merge:
            indices = containing_page_range.checkCounters()
            tailpage_setnumber, atIndex_tail = rid_decoder(rid_tail)
            for i in indices:
                update_column = i + FIRST_COLUMN
                bid_column = BID_TID_COLUMN
                schema_column = SCHEMA_ENCODING_COLUMN
                rid_column = RID_COLUMN

                is_full = containing_page_range.isFull()
                to_merge = (self, tailpage_setnumber, update_column, bid_column, schema_column, rid_column, atIndex_tail, containing_page_range)
                self.merge.addToQueue(is_full, containing_page_range, to_merge)

        return True

    def select_rid_data(self, rid, column):
        query_columns = [0] * self.num_columns
        query_columns[column] = 1
        return self.select(rid, query_columns)[0].columns[column]

    def select_record(self, index_value, index_column, query_columns):
        base_rids = self.index.locate(index_column, index_value)
        return self.select(base_rids, query_columns)

    def select(self, base_rids, query_columns):
        return_records = []
        for base_rid in base_rids:
            set_number, atIndex = rid_decoder(base_rid)
            baseSet = self._getFromDirectory(set_number)
            schema = baseSet.readPageAtIndex(SCHEMA_ENCODING_COLUMN, atIndex)
            tail_list = []
            column_results = [None] * self.num_columns
            # remember every base page has a tail page and every update goes to the taillist of the tail page.
            for i, query_col in enumerate(query_columns):
                if query_col:
                    if 1 << i & schema:  # if there is a schema matching therefore you added to the tail page.
                        tail_list.append(i)
                    else:
                        column_results[i] = baseSet.readPageAtIndex(FIRST_COLUMN + i,
                                                                    atIndex)  # read it from the base page.
            tail_rid = baseSet.readPageAtIndex(INDIRECTION_COLUMN, atIndex)
            tail_list.reverse()
            while len(tail_list):
                tail_setId, tail_index = rid_decoder(tail_rid)
                tail_set = self._getFromDirectory(tail_setId)
                tail_schema = tail_set.readPageAtIndex(SCHEMA_ENCODING_COLUMN, tail_index)
                for t in tail_list:
                    if 1 << t & tail_schema:
                        tail_list.remove(t)
                        column_results[t] = tail_set.readPageAtIndex(FIRST_COLUMN + t, tail_index)
                tail_rid = tail_set.readPageAtIndex(INDIRECTION_COLUMN, tail_index)
            return_records.append(Record(base_rid, self.key, column_results))
        return return_records

    def _getFromDirectory(self, page_set_id):
        self.page_directory_lock.acquire()
        baseSet = self.page_directory.get(page_set_id)
        if baseSet:
            self.page_directory_lock.release()
            return baseSet
        baseSet = PageSet(self.num_columns, self, activeRange=self.active_pageRange, set_number=page_set_id)
        self.page_directory[page_set_id] = baseSet
        self.page_directory_lock.release()
        return baseSet

    def delete_record(self, primary_key):
        # get record using using rid and page_directory
        rid = self.index.locate(self.key, primary_key)
        rid = rid[0]
        indirection = rid

        while rid != MAX_64_BITS and indirection != 0:
            set_number, atIndex = rid_decoder(indirection)
            pageSet = self._getFromDirectory(set_number)
            rid = pageSet.readPageAtIndex(RID_COLUMN, atIndex)
            indirection = pageSet.readPageAtIndex(INDIRECTION_COLUMN, atIndex)
            pageSet._getLockandWriteToPage(RID_COLUMN, atIndex, MAX_64_BITS)

        current_thread().addAction(target=self.index.delete, arguments=(primary_key)) #No Lock?
        return

    def get_record_column(self, rid, column):
        set_number, atIndex = rid_decoder(rid)
        page_set = self.page_directory[set_number]
        schema_flag = page_set.readPageAtIndex(SCHEMA_ENCODING_COLUMN, atIndex) & 1 << column
        indirection = None
        while schema_flag or indirection == rid:
            indirection = page_set.readPageAtIndex(INDIRECTION_COLUMN, atIndex)
            set_number, atIndex = rid_decoder(indirection)
            page_set = self.page_directory[set_number]
            if page_set.readPageAtIndex(SCHEMA_ENCODING_COLUMN, atIndex) & 1 << column:
                break
        return page_set.readPageAtIndex(FIRST_COLUMN + column, atIndex)

    def sum_range(self, start_range, end_range, aggregate_column_index):
        if start_range > end_range:
            return False
        rid_list = self.index.locate_range(start_range, end_range, self.key)
        if not len(rid_list):
            return False
        column_sum = 0
        for rid in rid_list:
            column_sum += self.get_record_column(rid, aggregate_column_index)
        return column_sum


def copy_page(page):
    return copy.deepcopy(page)
