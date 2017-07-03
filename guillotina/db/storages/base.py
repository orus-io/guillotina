from guillotina.db import TRASHED_ID


# we can not use FOR UPDATE or FOR SHARE unfortunately because
# it can cause deadlocks on the database--we need to resolve them ourselves
GET_OID = """
    SELECT zoid, tid, state_size, resource, of, parent_id, id, type, state
    FROM objects
    WHERE zoid = $1::varchar(32)
    """

GET_CHILDREN_KEYS = """
    SELECT id
    FROM objects
    WHERE parent_id = $1::varchar(32)
    """

GET_ANNOTATIONS_KEYS = """
    SELECT id
    FROM objects
    WHERE of = $1::varchar(32)
    """

GET_CHILD = """
    SELECT zoid, tid, state_size, resource, type, state, id
    FROM objects
    WHERE parent_id = $1::varchar(32) AND id = $2::text
    """

EXIST_CHILD = """
    SELECT zoid
    FROM objects
    WHERE parent_id = $1::varchar(32) AND id = $2::text
    """


HAS_OBJECT = """
    SELECT zoid
    FROM objects
    WHERE zoid = $1::varchar(32)
    """


GET_ANNOTATION = """
    SELECT zoid, tid, state_size, resource, type, state, id
    FROM objects
    WHERE of = $1::varchar(32) AND id = $2::text
    """


def _wrap_return_count(txt):
    return """WITH rows AS (
{}
    RETURNING 1
)
SELECT count(*) FROM rows""".format(txt)


# upsert without checking matching tids on updated object
NAIVE_UPSERT = """
INSERT INTO objects
(zoid, tid, state_size, part, resource, of, otid, parent_id, id, type, json, state)
VALUES ($1::varchar(32), $2::int, $3::int, $4::int, $5::boolean, $6::varchar(32), $7::int,
        $8::varchar(32), $9::text, $10::text, $11::json, $12::bytea)
ON CONFLICT (zoid)
DO UPDATE SET
    tid = EXCLUDED.tid,
    state_size = EXCLUDED.state_size,
    part = EXCLUDED.part,
    resource = EXCLUDED.resource,
    of = EXCLUDED.of,
    otid = EXCLUDED.otid,
    parent_id = EXCLUDED.parent_id,
    id = EXCLUDED.id,
    type = EXCLUDED.type,
    json = EXCLUDED.json,
    state = EXCLUDED.state"""
UPSERT = _wrap_return_count(NAIVE_UPSERT + """
    WHERE
        tid = EXCLUDED.otid""")
NAIVE_UPSERT = _wrap_return_count(NAIVE_UPSERT)


# update without checking matching tids on updated object
NAIVE_UPDATE = """
UPDATE objects
SET
    tid = $2::int,
    state_size = $3::int,
    part = $4::int,
    resource = $5::boolean,
    of = $6::varchar(32),
    otid = $7::int,
    parent_id = $8::varchar(32),
    id = $9::text,
    type = $10::text,
    json = $11::json,
    state = $12::bytea
WHERE
    zoid = $1::varchar(32)"""
UPDATE = _wrap_return_count(NAIVE_UPDATE + """ AND tid = $7::int""")
NAIVE_UPDATE = _wrap_return_count(NAIVE_UPDATE)


NEXT_TID = "SELECT nextval('tid_sequence');"
MAX_TID = "SELECT last_value FROM tid_sequence;"


NUM_CHILDREN = "SELECT count(*) FROM objects WHERE parent_id = $1::varchar(32)"


NUM_ROWS = "SELECT count(*) FROM objects"


NUM_RESOURCES = "SELECT count(*) FROM objects WHERE resource is TRUE"

NUM_RESOURCES_BY_TYPE = "SELECT count(*) FROM objects WHERE type=$1::TEXT"

RESOURCES_BY_TYPE = """
    SELECT zoid, tid, state_size, resource, type, state, id
    FROM objects
    WHERE type=$1::TEXT
    ORDER BY zoid
    LIMIT $2::int
    OFFSET $3::int
    """


GET_CHILDREN = """
    SELECT zoid, tid, state_size, resource, type, state, id
    FROM objects
    WHERE parent_id = $1::VARCHAR(32)
    """


TRASH_PARENT_ID = f"""
UPDATE objects
SET
    parent_id = '{TRASHED_ID}'
WHERE
    zoid = $1::varchar(32)
"""


INSERT_BLOB_CHUNK = """
    INSERT INTO blobs
    (bid, zoid, chunk_index, data)
    VALUES ($1::VARCHAR(32), $2::VARCHAR(32), $3::INT, $4::BYTEA)
"""


READ_BLOB_CHUNKS = """
    SELECT * from blobs
    WHERE bid = $1::VARCHAR(32)
    ORDER BY chunk_index
"""

READ_BLOB_CHUNK = """
    SELECT * from blobs
    WHERE bid = $1::VARCHAR(32)
    AND chunk_index = $2::int
"""


DELETE_BLOB = """
    DELETE FROM blobs WHERE bid = $1::VARCHAR(32);
"""


TXN_CONFLICTS = """
    SELECT zoid, tid, state_size, resource, type, id
    FROM objects
    WHERE tid > $1
    """


TXN_CONFLICTS_FULL = """
    SELECT zoid, tid, state_size, resource, type, state, id
    FROM objects
    WHERE tid > $1
    """

BATCHED_GET_CHILDREN_KEYS = """
    SELECT id
    FROM objects
    WHERE parent_id = $1::varchar(32)
    ORDER BY zoid
    LIMIT $2::int
    OFFSET $3::int
    """

DELETE_OBJECT = f"""
DELETE FROM objects
WHERE zoid = $1::varchar(32);
"""

GET_TRASHED_OBJECTS = f"""
SELECT zoid from objects where parent_id = '{TRASHED_ID}';
"""

CREATE_TRASH = f'''
INSERT INTO objects (zoid, tid, state_size, part, resource, type)
SELECT '{TRASHED_ID}', 0, 0, 0, 0, 'TRASH_REF'
WHERE NOT EXISTS (SELECT * FROM objects WHERE zoid = '{TRASHED_ID}')
'''


class BaseStorage(object):

    _cache_strategy = 'dummy'
    _read_only = False
    _transaction_strategy = 'resolve'

    def __init__(self, read_only=False, transaction_strategy='resolve',
                 cache_strategy='dummy'):
        self._read_only = read_only
        self._transaction_strategy = transaction_strategy
        self._cache_strategy = cache_strategy

    def read_only(self):
        return self._read_only
