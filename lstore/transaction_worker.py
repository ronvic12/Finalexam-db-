from lstore.threadManager import TransactionThread
from threading import current_thread as ct


class TransactionWorker:

    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions=None):
        self.stats = []
        self.transactions = transactions if transactions else []
        self.result = 0
        self.thread = TransactionThread(target=self.__run, daemon=True)

    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        self.transactions.append(t)

    """
    Runs all transaction as a thread
    """
    def run(self):
        self.thread.start()


    """
    Waits for the worker to finish
    """
    def join(self):
        self.thread.join()

    def __run(self):
        for transaction in self.transactions:
            # each transaction returns True if committed or False if aborted
            self.stats.append(transaction.run())
        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))


