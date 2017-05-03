from guillotina.db.interfaces import IStorage
from zope.interface import implementer


@implementer(IStorage)
class MockStorage:

    _cache = {}
    _read_only = False
    _transaction_strategy = 'merge'
    _options = {}

    def __init__(self, transaction_strategy='merge'):
        self._transaction_strategy = transaction_strategy

    async def get_annotation(self, trns, oid, id):
        return None


class MockManager:
    _storage = None

    def __init__(self, storage=None):
        if storage is None:
            storage = MockStorage()
        self._storage = storage
