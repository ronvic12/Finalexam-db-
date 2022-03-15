from threading import RLock as Lock
from threading import current_thread as ct
from lstore.abortException import ABORTEXCEPTION
from lstore.threadManager import TransactionThread


class LockManager:

	_instance = None

	managerlock = Lock()

	def __init__(self):
		raise Exception('Singleton, just call LockManager.getRIDLock(<rid>)')

	@classmethod
	def getRidLock(cls, rid, exclusive=False):
		cls.managerlock.acquire()
		if not cls._instance:
			cls._instance = LockManager.__new__(cls)
			cls._instance.RIDLocks = {}

		lock = cls._instance.RIDLocks.setdefault(rid, Lock())
		if ((type(ct()) is TransactionThread) and ct().hasLock(lock)) or lock.acquire(blocking=exclusive):
			cls.managerlock.release()
			return lock
		else:
			cls.managerlock.release()
			raise ABORTEXCEPTION(f"Could not acquire lock for rid {rid}, abort")



