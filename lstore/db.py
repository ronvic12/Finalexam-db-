import threading

from lstore.bufferPool import BufferPool
from lstore.config import *
from lstore.table import Table
from lstore.Merge import Merge
import pickle
import os
from lstore.lockManager import LockManager as lm

class Database:

    def __init__(self):
        self.tables = {}

    # Not required for milestone1
    def open(self, path):
        self.path = path
        BufferPool.kwargs = {'path': path}
        self._bufferPool = BufferPool.getBufferPool()
        self._merge = Merge.getMerge()
        self._mergeThread = threading.Thread(target=self._merge.startMerge, daemon=True)
        self._mergeThread.start()

    def close(self):
        self._merge.shutDown()
        for table in self.tables.values():
            self.writeTableToStorage(table)
        condition = True
        while condition:
            if self._mergeThread.is_alive():
                condition = False
        self._bufferPool.close()


    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index, index=None):
        table = Table(name, num_columns, key_index, index)
        self.tables[name] = table
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        self.tables.pop(name)

    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        if self.tables.__contains__(name):
            return self.tables.get(name)
        else:
            table_config, index = self.readTableFromStage(name)
            if not (table_config or index):
                return False
            return self.create_table(table_config['name'],
                                     table_config['num_columns'],
                                     table_config['key_index'],
                                     index=index)

    def writeTableToStorage(self, table: Table):
        config = {'name': table.name, 'num_columns': table.num_columns, 'key_index': table.key}
        if not os.path.exists(self.path):
            os.mkdir(self.path)
        writeDataToStorage(self._build_path(CONFIG_PATH), table.name, config)
        writeDataToStorage(self._build_path(INDEX_PATH), table.name, table.index.get_key_index_as_dic())

    def readTableFromStage(self, table_name):
        if not os.path.exists(self.path):
            os.mkdir(self.path)
        config = dict(readDataFromStorage(self._build_path(CONFIG_PATH), table_name))
        index = dict(readDataFromStorage(self._build_path(INDEX_PATH), table_name))
        return config, index

    def _build_path(self, sub_folder):
        return os.path.join(self.path, sub_folder)

def writeDataToStorage(path, name, data):
    if not os.path.exists(path):
        os.mkdir(path)
    filepath = path + name
    with open(filepath, 'wb') as file:
        pickle.dump(data, file)
        file.close()


def readDataFromStorage(path, name):
    if not os.path.exists(path):
        return None
    with open(path+name, 'rb') as file:
        return pickle.load(file)