# -*- encoding: utf-8 -*-
from aiohttp.test_utils import make_mocked_request
from guillotina.content import Folder
from guillotina.db import ROOT_ID
from guillotina.db.orm.interfaces import IBaseObject
from guillotina.db.transaction_manager import TransactionManager
from guillotina.interfaces import IDatabase
from zope.interface import implementer_only


@implementer_only(IDatabase, IBaseObject)
class Root(Folder):

    __name__ = None
    __cache__ = 0
    type_name = 'GuillotinaDBRoot'

    def __repr__(self):
        return "<Database %d>" % id(self)


class GuillotinaDB(object):

    def __init__(self,
                 storage,
                 database_name='unnamed'):
        """
        Create an object database.

        Database object is persistent through the application
        """
        self._tm = None
        self._storage = storage
        self._database_name = database_name
        self._tm = None

    @property
    def storage(self):
        return self._storage

    async def initialize(self):
        """
        create root object if necessary
        """
        request = make_mocked_request('POST', '/')
        request._db_write_enabled = True
        tm = request._tm = self.get_transaction_manager()
        txn = await tm.begin(request=request)
        # for get_current_request magic
        self.request = request

        try:
            assert tm.get(request=request) == txn
            await txn.get(ROOT_ID)
        except KeyError:
            root = Root()
            txn.register(root, new_oid=ROOT_ID)

        await tm.commit(txn=txn)

    async def open(self):
        """Return a database Connection for use by application code.
        """
        return await self._storage.open()

    async def close(self, conn):
        await self._storage.close(conn)

    async def finalize(self):
        await self._storage.finalize()

    def get_transaction_manager(self):
        """
        New transaction manager for every request
        """
        if self._tm is None:
            self._tm = TransactionManager(self._storage)
        return self._tm
