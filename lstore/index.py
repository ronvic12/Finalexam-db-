import secrets
from BTrees.IOBTree import IOBTree
from lstore.config import BITS_USED_FOR_SETID, DELETE_NODE
from threading import RLock as Lock

"""
A data structure holding indices for various columns of a table. Key column should be indexd by default,
other columns can be indexed through this object. Indices are usually B-Trees, but other data structures
can be used as well.
"""


class Index:

    def __init__(self, table, existing_index=None):
        self.table = table
        self.indices = [None] * table.num_columns
        self.indices[table.key] = IOBTree()
        self.indices[table.key].insert(DELETE_NODE, set())
        if existing_index:
            self._set_table_from_dic(existing_index, table.key)
        self.lock = Lock()

    def has_index(self, column):
        if self.indices[column]:
            return True
        else:
            return False

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        x = self.indices[column].get(value)
        if x:
            x = x.difference(self.indices[self.table.key][DELETE_NODE])
            return list(x)
        else:
            return []

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        rid_set = set()
        for ridSet in self.indices[column].keys(begin, end):
            rid_set = rid_set.union(self.indices[column].get(ridSet))
        return list(rid_set)

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        if self.indices[column_number]:
            return False
        self.indices[column_number] = IOBTree()
        values = self.indices[self.table.key].values()
        allValues = list(values)
        allValues.pop(0)
        for rid in allValues:
            data = self.table.select_rid_data(rid, column_number)
            self.add_to_index(column_number, data, *rid)
        sss = self.indices[self.table.key].values()
        return True

    """
    # optional: Drop index of specific column
    """
    def drop_index(self, column_number):
        if not column_number is self.table.key:
            self.indices[column_number] = None
            return True
        return False

    def add_to_index(self, column, key_value, rid):
        self.lock.acquire()
        if self.indices[column] is None:
            self.lock.release()
            return False
        if key_value in self.indices[column]:
            self.indices[column][key_value].add(rid)
        else:
            self.indices[column].insert(key_value, {rid})
        self.lock.release()
        return True

    def delete(self, key):
        if key in self.indices[self.table.key]:
            rid = self.indices[self.table.key].pop(key)
            # Will have merge clean
            """column_data = self.table.select(rid, [1]*self.table.num_columns)
            for tree, column in zip(self.indices, column_data):
                if tree and self.indices[self.table.key] is not tree:
                    tree[column].remove(rid)"""
            self.indices[self.table.key][DELETE_NODE].add(*rid)
            return True
        else:
            return False

    def update(self, column, rid, old_value, new_value):
        #     Cannot update key     and    insure index is created
        self.lock.acquire()
        if column != self.table.key and self.indices[column] is not None:
            if old_value in self.indices[column]:
                self.indices[column][old_value].remove(rid)
                if new_value in self.indices[column]:
                    self.indices[column][new_value].add(rid)
                else:
                    self.indices[column].insert(new_value, {rid})
        self.lock.release()


    def get_new_set_number(self):
        return secrets.randbits(BITS_USED_FOR_SETID)
        # while True:
        #     set_id = secrets.randbits(48)
        #     if not self.used_setID.__contains__(set_id):
        #         self.used_setID.add(set_id)
        #         return set_id

    def get_key_index_as_dic(self):
        returnDIct = {}
        for index in self.indices[self.table.key].iteritems():
            returnDIct.__setitem__(*index)
        return returnDIct

    def _set_table_from_dic(self,index_dict: dict, key_columns):
        for entry in index_dict:
            self.indices[key_columns].insert(entry, index_dict[entry])