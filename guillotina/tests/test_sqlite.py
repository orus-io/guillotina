from guillotina.db.storages.sqlite import SQLiteStorage
from guillotina.db.transaction_manager import TransactionManager
from guillotina.tests.utils import create_content

import os
import tempfile


async def cleanup(storage):
    os.remove(storage._dsn)


async def get_sqlite(strategy='none'):
    _, dsn = tempfile.mkstemp()
    storage = SQLiteStorage(
        dsn=dsn, name='db',
        transaction_strategy=strategy,
        conn_acquire_timeout=0.1)
    await storage.initialize()
    return storage


async def test_read_obs(postgres, dummy_request):
    """Low level test checks that root is not there"""
    request = dummy_request  # noqa so magically get_current_request can find

    storage = await get_sqlite()
    tm = TransactionManager(storage)
    txn = await tm.begin()

    ob = create_content()
    txn.register(ob)

    assert len(txn.modified) == 1

    await tm.commit(txn=txn)

    txn = await tm.begin()

    ob2 = await txn.get(ob._p_oid)

    assert ob2._p_oid == ob._p_oid
    await tm.commit(txn=txn)

    await storage.remove()
    await cleanup(storage)
