from lstore.config import *
from lstore.pageSet import PageSet

class PageRange:

    def __init__(self, num_columns, table, create_empty_set=True):
        self.sets = {}
        self.tailSets = []
        self.range_size = PAGE_SETS_PER_TAIL
        self.table = table
        self.tailSet = PageSet(num_columns, table, set_number=self.table.get_new_pageset_num(), activeRange=self)
        self.num_columns = num_columns
        self.table.addToPageDirectory(self.tailSet)  # ARE WE STILL ADDING THE POINTER WITH THE CURRENT IMPLEMENTATION?
        tailSet_id = self.tailSet.getSetNumber()
        self.tailSets.append(tailSet_id)
        if create_empty_set:
            self.activeSet = PageSet(num_columns, table, set_number=self.table.get_new_pageset_num(), activeRange=self)
            self.sets[self.activeSet.set_number] = self.activeSet
        else:
            self.activeSet = None
        self.counters = [MERGE_TRIGGER for _ in range(num_columns)]
        self.full = False

    def setFull(self):
        self.full = True

    def isFull(self):
        return self.full

    def obtainTailIndex(self, tailPage):
        return self.tailSets.index(tailPage)

    def obtainTailPage(self, atIndex):
        return self.tailSets[atIndex]

    def obtainPreviousTailPage(self, tailPageNumber):
        atIndex = self.tailSets.index(tailPageNumber)
        return self.tailSets[atIndex - 1]

    def getTail(self):
        return self.tailSet

    def getPageSets(self):
        set_data = {}
        for s in self.sets.values():
            set_data[s.set_number] = (s, self)
        return set_data

    def addSet(self, pagesetset: PageSet):
        if not len(self.sets) < PAGE_SETS_PER_TAIL:
            return False
        pagesetset.attachToRange(self)
        self.sets[pagesetset.set_number] = pagesetset

    def addNewRecord(self, command):
        while True:
            rid = self.activeSet.addNewRecord(command)
            if rid:
                # if active is success will return int RID and pointer to set
                return rid, self.activeSet
            else:
                if len(self.sets) >= PAGE_SETS_PER_TAIL:
                    return False
                self.addSet(self.activeSet)
                self.activeSet = PageSet(self.num_columns, self.table, activeRange=self)

    def addToTail(self, indirection, commands, bid, baseSet):
        write = []
        old_results = [None] * len(commands)
        schema = 0
        for i, c in enumerate(commands):
            if c is not None:
                schema = schema | 1 << i
                write.append(c)
            else:
                write.append(0)

        while True:
            rid = self.tailSet.addNewRecord(write, bid=bid, indirection=indirection, schema=schema)
            if rid:
                return rid, schema
            else:
                self.tailSet = PageSet(self.num_columns, self.table, activeRange=self)
                self.table.addToPageDirectory(self.tailSet)
                tailSet_id = self.tailSet.getSetNumber()
                self.tailSets.append(tailSet_id)

    def decreaseCounter(self, at_Index):
        self.counters[at_Index] -= 1
        return self.counters[at_Index]

    def checkCounters(self):
        indices=[]
        for i in range(len(self.counters)):
            if self.counters[i] == 0:
                self.counters[i] = MERGE_TRIGGER
                indices.append(i)
        return indices

