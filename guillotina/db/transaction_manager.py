from asyncio import shield
from guillotina import glogging
from guillotina.db import ROOT_ID
from guillotina.db.transaction import Status
from guillotina.db.transaction import Transaction
from guillotina.exceptions import ConflictError
from guillotina.exceptions import RequestNotFound
from guillotina.exceptions import TIDConflictError
from guillotina.profile import profilable
from guillotina.utils import get_authenticated_user_id
from guillotina.utils import get_current_request

import asyncpg


logger = glogging.getLogger('guillotina')


def _get_conn_query_count(conn):
    '''
    diff versions of asyncpg
    '''
    try:
        return conn._protocol.queries_count
    except Exception:
        try:
            return conn._con._protocol.queries_count
        except Exception:
            pass
    return 0


class TransactionManager(object):
    """
    Transaction manager for storing the managed transaction in the
    current request object.
    """

    def __init__(self, storage):
        # Guillotine Storage
        self._storage = storage
        # Pointer to last transaction created
        self._last_txn = None
        # Pointer to last db connection opened
        self._last_db_conn = None

    async def get_root(self, txn=None):
        if txn is None:
            txn = self._last_txn
        return await txn.get(ROOT_ID)

    @profilable
    async def begin(self, request=None):
        """Starts a new transaction.
        """

        if request is None:
            try:
                request = get_current_request()
            except RequestNotFound:
                pass

        user = None

        txn = None

        # already has txn registered, as long as connection is closed, it
        # is safe
        if (getattr(request, '_txn', None) is not None and
                request._txn.status in (Status.ABORTED, Status.COMMITTED, Status.CONFLICT)):
            # re-use txn if possible
            txn = request._txn
            txn.status = Status.ACTIVE
            if txn._db_conn is not None:
                try:
                    await self._close_txn(txn)
                except Exception:
                    logger.warn('Unable to close spurious connection', exc_info=True)
        else:
            txn = Transaction(self, request=request)

        self._last_txn = txn

        if request is not None:
            # register tm and txn with request
            request._tm = self
            request._txn = txn
            user = get_authenticated_user_id(request)

        if user is not None:
            txn.user = user

        db_conn = self._last_db_conn = await self._storage.open()
        txn._query_count_start = _get_conn_query_count(db_conn)
        await txn.tpc_begin(db_conn)

        return txn

    async def commit(self, request=None, txn=None):
        return await shield(self._commit(request=request, txn=txn))

    async def _commit(self, request=None, txn=None):
        """ Commit the last transaction
        """
        if txn is None:
            txn = self.get(request=request)
        if txn is not None:
            try:
                await txn.commit()
            except (ConflictError, TIDConflictError):
                # # we're okay with ConflictError being handled...
                txn.status = Status.CONFLICT
                raise
            except Exception:
                logger.error('Error committing transaction {}'.format(txn._tid),
                             exc_info=True)
            finally:
                await self._close_txn(txn)
        else:
            await self._close_txn(txn)

    async def _close_txn(self, txn):
        if txn is not None and txn._db_conn is not None:
            try:
                txn._query_count_end = _get_conn_query_count(txn._db_conn)
            except AttributeError:
                pass
            try:
                await self._storage.close(txn._db_conn)
            except asyncpg.exceptions.InterfaceError as ex:
                if 'received invalid connection' in str(ex):
                    # ignore, new pool was created so we can not close this conn
                    pass
                else:
                    raise
            finally:
                txn._db_conn = None
        if txn == self._last_txn:
            self._last_txn = None
            self._last_db_conn = None

    async def abort(self, request=None, txn=None):
        return await shield(self._abort(request=request, txn=txn))

    async def _abort(self, request=None, txn=None):
        """ Abort the last transaction
        """
        if txn is None:
            txn = self.get(request=request)
        if txn is not None:
            await txn.abort()
        await self._close_txn(txn)

    def get(self, request=None):
        """Return the current request specific transaction
        """
        if request is None:
            try:
                request = get_current_request()
            except RequestNotFound:
                pass
        if request is None:
            return self._last_txn
        return request._txn
