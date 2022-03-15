from threading import current_thread as ct
from lstore.abortException import ABORTEXCEPTION



class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table,  *args):
        self.queries.append((query, args))
        # use grades_table for aborting

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args in self.queries:
            try:
                query(*args)
            except ABORTEXCEPTION:
                return self.abort()
            # If the query has failed the transaction should abort
            ct().execute()
            ct().releaseLocks()
        return self.commit()

    def abort(self):
        return False

    def commit(self):
        return True


