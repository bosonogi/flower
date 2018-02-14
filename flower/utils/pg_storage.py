import json
import queue
import logging
import threading
from datetime import datetime
from socket import error as socketerror

import pg8000

logger = logging.getLogger(__name__)
connection = None
_connection_options = {}
_db_insert_queue = queue.Queue()
_db_insert_thread = None
skip_callback = False

REQ_MAX_RETRIES = 2

_all_tables = """
SELECT * FROM information_schema.tables
WHERE table_schema = 'public'
"""

_schema = (
    """CREATE TABLE events
    (
        id SERIAL PRIMARY KEY,
        time TIMESTAMP NOT NULL,
        uuid VARCHAR(36) NOT NULL,
        data TEXT NOT NULL,
        unique (time, data)
    )""",
    "CREATE INDEX event_time_index ON events (time ASC)",
    "CREATE INDEX event_uuid_index ON events (uuid ASC)",
)

_add_event = """INSERT INTO events (time, uuid, data) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING"""

_get_max_events = """
                    SELECT data FROM (
                        SELECT *
                        FROM events
                        ORDER BY id DESC
                        LIMIT {max_events}
                    ) subevents
                    ORDER BY time ASC
                    """

_get_task_events = """
                    SELECT data
                    FROM events
                    WHERE uuid IN (
                        SELECT uuid FROM (
                            SELECT DISTINCT ON (uuid) uuid, time
                            FROM events
                            ORDER BY uuid, time DESC
                        ) uuid_list
                        ORDER BY time DESC
                        LIMIT {max_tasks}
                    )
                    ORDER BY time ASC
                    """

_ignored_events = {
    'worker-offline',
    'worker-online',
    'worker-heartbeat',
}


def _execute_query(query, args=None, rollback_on_error=True):
    cursor = connection.cursor()
    for attempt in range(REQ_MAX_RETRIES):
        try:
            cursor.execute(query, args)
            connection.commit()

        except (socketerror, pg8000.InterfaceError):
            logger.warning('Flower encountered a connection error with PostGreSQL database. Retrying.')
            open_connection(**_connection_options)
            cursor = connection.cursor()
            continue

        except Exception:
            if rollback_on_error:
                connection.rollback()
            raise

        finally:
            cursor.close()

        break

    else:
        logger.exception('Flower encountered a connection error with PostGreSQL database.')
        raise RuntimeError('Unable to execute query after {} tries: {} (args={})'.format(
            REQ_MAX_RETRIES, query, args)
        )

    return cursor


def insert_worker():
    while True:
        args = _db_insert_queue.get()
        _execute_query(_add_event, args)
        logger.debug('Added event %s to PostGreSQL persistence store', args[1])


def event_callback(state, event):
    global _db_insert_thread

    if skip_callback or event['type'] in _ignored_events:
        return

    if _db_insert_thread is None:
        _db_insert_thread = threading.Thread(target=insert_worker, daemon=True)
        _db_insert_thread.start()

    _db_insert_queue.put((
        datetime.fromtimestamp(event['timestamp']),
        event['uuid'],
        json.dumps(event)
    ))


def open_connection(user, password, database, host, port, use_ssl):
    global connection
    global _connection_options
    connection = pg8000.connect(
        user=user, password=password, database=database,
        host=host, port=port, ssl=use_ssl
    )
    _connection_options = {'user': user,
                           'password': password,
                           'database': database,
                           'host': host,
                           'port': port,
                           'use_ssl': use_ssl
                           }


def maybe_create_schema():
    global connection
    # Create schema if table is missing
    cursor = connection.cursor()
    try:
        cursor.execute(_all_tables)
        tables = cursor.fetchall()

        if tables is None or not any(('events' in table[2]) for table in tables):
            logger.debug('Table events missing, executing schema definition.')
            for statement in _schema:
                cursor.execute(statement)
            connection.commit()

    finally:
        cursor.close()


def close_connection():
    global connection
    if connection is not None:
        connection.close()
        connection = None


def get_events(max_events, max_tasks):
    logger.debug('Events loading from postgresql persistence backend')
    if max_events:
        if max_events == -1:
            query = _get_max_events.format(max_events='ALL')
        else:
            query = _get_max_events.format(max_events=max_events)
    else:
        query = _get_task_events.format(max_tasks=max_tasks)

    cursor = _execute_query(query)
    for row in cursor:
        yield json.loads(row[0])
    logger.debug('{} Events loaded from postgresql persistence backend'.format(cursor.rowcount))
