from asyncio import shield
from guillotina.db.interfaces import IStorage
from guillotina.db.storages import base, pg
from guillotina.db.storages.utils import get_table_definition
from guillotina.exceptions import ConflictError
from guillotina.exceptions import TIDConflictError
from zope.interface import implementer

import asyncio
import concurrent
import logging
import ujson, sqlite3


log = logging.getLogger("guillotina.storage")


MAX_TID = "SELECT COALESCE(MAX(tid), 0) from objects;"


class SQLiteVacuum:

    def __init__(self, storage, loop):
        self._storage = storage
        self._loop = loop
        self._queue = asyncio.Queue(loop=loop)
        self._active = False
        self._closed = False

    @property
    def active(self):
        return self._active

    async def initialize(self):
        # get existing trashed objects, push them on the queue...
        # there might be contention, but that is okay
        conn = self._storage._read_conn
        try:
            curse = conn.execute(base.GET_TRASHED_OBJECTS)
            for record in curse.fetchall():
                await self._queue.put(record['zoid'])
        except:
            log.warn('Error deleting trashed object', exc_info=True)

        while not self._closed:
            try:
                oid = await self._queue.get()
                self._active = True
                await self.vacuum(oid)
            except (concurrent.futures.CancelledError, RuntimeError):
                pass  # task was cancelled, probably because we're shutting down
            except Exception:
                log.warning(f'Error vacuuming oid {oid}', exc_info=True)
            finally:
                self._active = False
                try:
                    self._queue.task_done()
                except ValueError:
                    pass

    async def add_to_queue(self, oid):
        await self._queue.put(oid)

    async def vacuum(self, oid):
        '''
        DELETED objects has parent id changed to the trashed ob for the oid...
        '''
        conn = self._storage._read_conn
        try:
            curse = conn.execute(base.DELETE_OBJECT, oid)
            curse.fetchall()
        except:
            log.warn('Error deleting trashed object', exc_info=True)

    async def finalize(self):
        self._closed = True
        await self._queue.join()
        # wait for up to two seconds to finish the task...
        # it's not long but we don't want to wait for a long time to close either....
        try:
            await asyncio.wait_for(self.wait_until_no_longer_active(), 2)
        except concurrent.futures.CancelledError:
            # we do not care if it's cancelled... things will get cleaned up
            # in a future task anyways...
            pass

    async def wait_until_no_longer_active(self):
        while self._active:
            # give it a chance to finish...
            await asyncio.sleep(0.1)


class SQLiteTransaction:

    def __init__(self, txn):
        self._txn = txn
        self._conn = txn._db_conn
        self._storage = txn._manager._storage
        self._status = 'none'

    async def start(self):
        assert self._status in ('none',)
        self._conn.execute(f'''BEGIN TRANSACTION;''')
        self._status = 'started'

    async def commit(self):
        assert self._status in ('started',)
        self._conn.execute('COMMIT;')
        self._status = 'committed'

    async def rollback(self):
        assert self._status in ('started',)
        self._conn.execute('ROLLBACK;')
        self._status = 'rolledback'


@implementer(IStorage)
class SQLiteStorage(pg.PostgresqlStorage):
    """Storage to sqlite"""

    _loop = None
    _vacuum_class = SQLiteVacuum
    _initialize_statements = [
        'CREATE INDEX IF NOT EXISTS object_tid ON objects (tid);',
        'CREATE INDEX IF NOT EXISTS object_of ON objects (of);',
        'CREATE INDEX IF NOT EXISTS object_part ON objects (part);',
        'CREATE INDEX IF NOT EXISTS object_parent ON objects (parent_id);',
        'CREATE INDEX IF NOT EXISTS object_id ON objects (id);',
        'CREATE INDEX IF NOT EXISTS blob_bid ON blobs (bid);',
        'CREATE INDEX IF NOT EXISTS blob_zoid ON blobs (zoid);',
        'CREATE INDEX IF NOT EXISTS blob_chunk ON blobs (chunk_index);'
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._read_conn = sqlite3.connect(kwargs['dsn'])

    async def finalize(self):
        await self._vacuum.finalize()
        self._vacuum_task.cancel()
        self._db_conn.close()
        self._read_conn.close()

    async def create(self):
        # Check DB
        log.info('Creating initial database objects')
        statements = [
            get_table_definition('objects', self._object_schema),
            get_table_definition('blobs', self._blob_schema,
                                 primary_keys=('bid', 'zoid', 'chunk_index'))
        ]
        statements.extend(self._initialize_statements)

        for statement in statements:
            curse = self._read_conn.execute(statement)
            curse.fetchall()

    async def initialize(self, loop=None):
        if loop is None:
            loop = asyncio.get_event_loop()
        self._loop = loop

        # shared read connection on all transactions
        await self.create()

        self._read_conn.execute(base.CREATE_TRASH)

        self._vacuum = self._vacuum_class(self, loop)
        self._vacuum_task = asyncio.Task(self._vacuum.initialize(), loop=loop)

        def vacuum_done(task):
            if self._vacuum._closed:
                # if it's closed, we know this is expected
                return
            log.warning('Vacuum pg task closed. This should not happen. '
                        'No database vacuuming will be done here anymore.')

        self._vacuum_task.add_done_callback(vacuum_done)

    async def remove(self):
        """Reset the tables"""
        async with self._pool.acquire() as conn:
            await conn.execute("DROP TABLE IF EXISTS blobs;")
            await conn.execute("DROP TABLE IF EXISTS objects;")

    async def open(self):
        return sqlite3.connect(self._dsn)

    async def close(self, con):
        con.close()

    async def load(self, txn, oid):
        async with txn._lock:
            smt = await txn._db_conn.prepare(base.GET_OID)
            objects = await smt.fetchrow(oid)
        if objects is None:
            raise KeyError(oid)
        return objects

    async def store(self, oid, old_serial, writer, obj, txn):
        assert oid is not None

        p = writer.serialize()  # This calls __getstate__ of obj
        if len(p) >= self._large_record_size:
            self._log.warning("Too long object %d" % (obj.__class__, len(p)))
        json_dict = await writer.get_json()
        json = ujson.dumps(json_dict)
        part = writer.part
        if part is None:
            part = 0

        update = False
        statement_sql = base.NAIVE_UPSERT
        if not obj.__new_marker__ and obj._p_serial is not None:
            # we should be confident this is an object update
            statement_sql = base.UPDATE
            update = True

        async with txn._lock:
            smt = await txn._db_conn.prepare(statement_sql)
            try:
                result = await smt.fetch(
                    oid,                 # The OID of the object
                    txn._tid,            # Our TID
                    len(p),              # Len of the object
                    part,                # Partition indicator
                    writer.resource,     # Is a resource ?
                    writer.of,           # It belogs to a main
                    old_serial,          # Old serial
                    writer.parent_id,    # Parent OID
                    writer.id,           # Traversal ID
                    writer.type,         # Guillotina type
                    json,                # JSON catalog
                    p                    # Pickle state)
                )
            except sqlite3.DatabaseError:
                import pdb; pdb.set_trace()
            if len(result) != 1 or result[0]['count'] != 1:
                if update:
                    # raise tid conflict error
                    raise TIDConflictError(
                        'Mismatch of tid of object being updated. This is likely '
                        'caused by a cache invalidation race condition and should '
                        'be an edge case. This should resolve on request retry.')
                else:
                    self._log.error('Incorrect response count from database update. '
                                    'This should not happen. tid: {}'.format(txn._tid))

    async def _txn_oid_commit_hook(self, status, oid):
        await self._vacuum.add_to_queue(oid)

    async def delete(self, txn, oid):
        async with txn._lock:
            # for delete, we reassign the parent id and delete in the vacuum task
            await txn._db_conn.execute(base.TRASH_PARENT_ID, oid)
        txn.add_after_commit_hook(self._txn_oid_commit_hook, [oid])

    async def get_next_tid(self, txn):
        async with self._lock:
            # we do not use transaction lock here but a storage lock because
            # a storage object has a shard conn for reads
            return await self.get_current_tid(txn) + 1

    async def get_current_tid(self, txn):
        async with self._lock:
            # again, use storage lock here instead of trns lock
            curse = self._read_conn.execute(MAX_TID)
            return curse.fetchval()

    def _db_transaction_factory(self, txn):
        return SQLiteTransaction(txn)

    async def start_transaction(self, txn):
        async with txn._lock:
            txn._db_txn = self._db_transaction_factory(txn)
            await txn._db_txn.start()

    async def get_conflicts(self, txn, full=False):
        async with self._lock:
            # use storage lock instead of transaction lock
            if full:
                return await self._read_conn.fetch(base.TXN_CONFLICTS_FULL, txn._tid)
            else:
                return await self._read_conn.fetch(base.TXN_CONFLICTS, txn._tid)

    async def commit(self, transaction):
        if transaction._db_txn is not None:
            async with transaction._lock:
                await transaction._db_txn.commit()
        elif self._transaction_strategy not in ('none', 'tidonly'):
            log.warning('Do not have db transaction to commit')
        return transaction._tid

    async def abort(self, transaction):
        if transaction._db_txn is not None:
            async with transaction._lock:
                try:
                    await transaction._db_txn.rollback()
                except sqlite3.DatabaseError:
                    # we're okay with this error here...
                    pass
        # reads don't need transaction necessarily so don't log
        # else:
        #     log.warning('Do not have db transaction to rollback')

    # Introspection
    async def get_page_of_keys(self, txn, oid, page=1, page_size=1000):
        conn = txn._db_conn
        smt = await conn.prepare(base.BATCHED_GET_CHILDREN_KEYS)
        keys = []
        for record in await smt.fetch(oid, page_size, (page - 1) * page_size):
            keys.append(record['id'])
        return keys

    async def keys(self, txn, oid):
        async with txn._lock:
            smt = await txn._db_conn.prepare(base.GET_CHILDREN_KEYS)
            result = await smt.fetch(oid)
        return result

    async def get_child(self, txn, parent_oid, id):
        async with txn._lock:
            smt = await txn._db_conn.prepare(base.GET_CHILD)
            result = await smt.fetchrow(parent_oid, id)
        return result

    async def has_key(self, txn, parent_oid, id):
        async with txn._lock:
            smt = await txn._db_conn.prepare(base.EXIST_CHILD)
            result = await smt.fetchrow(parent_oid, id)
        if result is None:
            return False
        else:
            return True

    async def len(self, txn, oid):
        async with txn._lock:
            smt = await txn._db_conn.prepare(base.NUM_CHILDREN)
            result = await smt.fetchval(oid)
        return result

    async def items(self, txn, oid):
        async with txn._lock:
            smt = await txn._db_conn.prepare(base.GET_CHILDREN)
        async for record in smt.cursor(oid):
            # locks are dangerous in cursors since comsuming code might do
            # sub-queries and they you end up with a deadlock
            yield record

    async def get_annotation(self, txn, oid, id):
        async with txn._lock:
            smt = await txn._db_conn.prepare(base.GET_ANNOTATION)
            result = await smt.fetchrow(oid, id)
        return result

    async def get_annotation_keys(self, txn, oid):
        async with txn._lock:
            smt = await txn._db_conn.prepare(base.GET_ANNOTATIONS_KEYS)
            result = await smt.fetch(oid)
        return result

    async def write_blob_chunk(self, txn, bid, oid, chunk_index, data):
        async with txn._lock:
            smt = await txn._db_conn.prepare(base.HAS_OBJECT)
            result = await smt.fetchrow(oid)
        if result is None:
            # check if we have a referenced ob, could be new and not in db yet.
            # if so, create a stub for it here...
            async with txn._lock:
                await txn._db_conn.execute('''INSERT INTO objects
                    (zoid, tid, state_size, part, resource, type)
                    VALUES ($1::varchar(32), -1, 0, 0, TRUE, 'stub')''', oid)
        async with txn._lock:
            return await txn._db_conn.execute(
                base.INSERT_BLOB_CHUNK, bid, oid, chunk_index, data)

    async def read_blob_chunk(self, txn, bid, chunk=0):
        async with txn._lock:
            smt = await txn._db_conn.prepare(base.READ_BLOB_CHUNK)
            return await smt.fetchrow(bid, chunk)

    async def read_blob_chunks(self, txn, bid):
        async with txn._lock:
            smt = await txn._db_conn.prepare(base.READ_BLOB_CHUNKS)
        async for record in smt.cursor(bid):
            # locks are dangerous in cursors since comsuming code might do
            # sub-queries and they you end up with a deadlock
            yield record

    async def del_blob(self, txn, bid):
        async with txn._lock:
            await txn._db_conn.execute(base.DELETE_BLOB, bid)

    async def get_total_number_of_objects(self, txn):
        async with txn._lock:
            smt = await txn._db_conn.prepare(base.NUM_ROWS)
            result = await smt.fetchval()
        return result

    async def get_total_number_of_resources(self, txn):
        async with txn._lock:
            smt = await txn._db_conn.prepare(base.NUM_RESOURCES)
            result = await smt.fetchval()
        return result

    async def get_total_resources_of_type(self, txn, type_):
        async with txn._lock:
            smt = await txn._db_conn.prepare(base.NUM_RESOURCES_BY_TYPE)
            result = await smt.fetchval(type_)
        return result

    # Massive treatment without security
    async def _get_page_resources_of_type(self, txn, type_, page, page_size):
        conn = txn._db_conn
        async with txn._lock:
            smt = await conn.prepare(base.RESOURCES_BY_TYPE)
        keys = []
        for record in await smt.fetch(type_, page_size, (page - 1) * page_size):  # noqa
            keys.append(record)
        return keys
