import threading
from threading import Thread
from threading import current_thread


class TransactionThread(Thread):

	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		self.transaction_data = []
		self.current_query_data = None
		self.all_locks = set()

	def startNewQueryDataInstance(self, query_type):
		self.current_query_data = QueryData(query_type)
		self.transaction_data.append(self.current_query_data)

	def current_thread(self):
		return threading.current_thread()

	def addAction(self, target, arguments, lock=None):
		if lock: self.addLock(lock)
		self.current_query_data.addNewQuery(target, arguments, lock)

	def hasLock(self, lock):
		return lock in self.all_locks

	def addLock(self, lock):
		self.all_locks.add(lock)

	def execute(self):
		for query in self.transaction_data:
			query.execute()

	def releaseLocks(self):
		for lock in self.all_locks:
				lock.release()
		self.all_locks.clear()


class QueryData:

	def __init__(self, type):
		self.type = type
		self.pending_actions = []

	def addNewQuery(self, target, arguments, lock):
		self.pending_actions.append(QueryAction(target, arguments, lock))

	def execute(self):
		while self.pending_actions:
			action = self.pending_actions.pop(0)
			action.execute()


class QueryAction:

	def __init__(self, target, arguments, lock):
		self.target = target
		self.arguments = arguments
		self.lock = lock
		
	def execute(self):
		self.target(*self.arguments)


def initializeQuery(query_type):
	current_thread().startNewQueryDataInstance(query_type)
