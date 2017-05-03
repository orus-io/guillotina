from guillotina import configure
from guillotina.db import etcd
from guillotina.db.interfaces import ILockingStrategy
from guillotina.db.interfaces import IStorage
from guillotina.db.interfaces import ITransaction

import asyncio
import json


@configure.adapter(
    for_=(IStorage, ITransaction),
    provides=ILockingStrategy, name="lock")
class LockStrategy:
    '''
    *this strategy relies on using etcd for locking*

    A transaction strategy that depends on locking objects in order to safely
    write to them.

    Application logic needs to implement the object locking.

    Unlocking should be done in the tpc_finish phase.
    '''

    def __init__(self, storage, transaction):
        self._storage = storage
        self._transaction = transaction

        options = storage._options
        self._lock_ttl = options.get('lock_ttl', 3)
        etcd_options = options.get('etcd', {})
        self._etcd_base_key = etcd_options.pop('base_key', 'guillotina-')
        self._etcd_acquire_timeout = etcd_options.pop('acquire_timeout', 3)

        if not hasattr(self._storage, '_etcd_client'):
            self._storage._etcd_client = etcd.Client(**etcd_options)
        self._etcd_client = self._storage._etcd_client

    async def tpc_begin(self):
        key = '{}-tid'.format(self._etcd_base_key)
        tid = 1
        tries = 0  # try to get new tid
        while True:
            # get current tid
            result = await self._etcd_client.get(key)
            if 'errorCode' in result:
                if result['errorCode'] == 100:
                    # no key, just set and we're good
                    await self._etcd_client.set(key, tid, noValueOnSuccess='true')
                    break
                else:
                    raise Exception('Could not allocate tid for transaction. etcd error {}'.format(
                        json.dumps(result)
                    ))
            else:
                existing_tid = result['node']['value']
                tid = int(existing_tid) + 1
                result = await self._etcd_client.set(key, tid, prevValue=existing_tid)
                if 'errorCode' in result:
                    if result['errorCode'] != 101:
                        # 101 is okay, we'll retry. Others, throw unhandled errorCode
                        raise Exception('Could not allocate tid for transaction. etcd error {}'.format(
                            json.dumps(result)
                        ))
                else:
                    # success!, break out
                    break
            tries += 1
            if tries >= 10:
                raise Exception('Could not allocate tid for transaction, too busy'.format(
                    json.dumps(result)
                ))
        self._transaction._tid = tid

    async def tpc_vote(self):
        """
        Never a problem for voting since we're relying on locking
        """
        return True

    async def tpc_finish(self):
        for ob in self._transaction.modified.values():
            if ob.__locked__:
                await self.unlock(ob)

    def _get_key(self, ob):
        return '{}-{}-lock'.format(self._etcd_base_key, ob._p_oid)

    async def _wait_for_lock(self, key):
        # this method *should* use the wait_for with a timeout
        result = await self._etcd_client.get(key)
        if 'node' in result:
            if result['node']['value'] == 'locked':
                asyncio.sleep(0.01)  # sleep a bit and try again...
                return await self._wait_for_lock(key)
            else:
                result = await self._etcd_client.set(
                    key, 'locked', ttl=self._lock_ttl,
                    prevIndex=result['node']['modifiedIndex'])
                if 'errorCode' in result:
                    asyncio.sleep(0.01)  # sleep a bit and try again...
                    return await self._wait_for_lock(key)
                else:
                    return result
        else:
            result = await self._etcd_client.set(
                key, 'locked', ttl=self._lock_ttl,
                prevExist='false')
            if 'errorCode' in result:
                asyncio.sleep(0.01)  # sleep a bit and try again...
                return await self._wait_for_lock(key)
            else:
                return result

    async def lock(self, obj):
        assert not obj.__new_marker__  # should be modifying an object
        if obj.__locked__:  # we've already locked this...
            return

        obj.__locked__ = True
        if obj._p_oid not in self._transaction.modified:
            # need to added it when locking...
            self._transaction.modified[obj._p_oid] = obj
        key = self._get_key(obj)

        try:
            await asyncio.wait_for(
                self._wait_for_lock(key),
                timeout=self._etcd_acquire_timeout)
        except asyncio.TimeoutError:
            raise Exception('Could not lock ob for writing')

    async def unlock(self, obj):
        if not obj.__locked__:
            # already unlocked
            return
        obj.__locked__ = False
        key = self._get_key(obj)
        await self._etcd_client.set(key, 'unlocked', ttl=self._lock_ttl)
