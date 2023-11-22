#!/usr/bin/env python3
import datetime
import json
import sqlite3
from sqlite3 import apilevel  # noqa
from sqlite3 import DatabaseError  # noqa
from sqlite3 import DataError  # noqa
from sqlite3 import Error  # noqa
from sqlite3 import IntegrityError  # noqa
from sqlite3 import InterfaceError  # noqa
from sqlite3 import InternalError  # noqa
from sqlite3 import NotSupportedError  # noqa
from sqlite3 import OperationalError  # noqa
from sqlite3 import paramstyle  # noqa
from sqlite3 import ProgrammingError  # noqa
from sqlite3 import threadsafety  # noqa
from sqlite3 import Warning  # noqa
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

CACHE_NAME = 'file:fusion?mode=memory&cache=shared'
SCHEMA = r'''
    BEGIN;

    CREATE TABLE IF NOT EXISTS fusion.Regions (
        regionID TEXT PRIMARY KEY NOT NULL,
        region TEXT NOT NULL,
        provider TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS fusion.WorkspaceGroups (
        workspaceGroupID TEXT PRIMARY KEY NOT NULL,
        name TEXT NOT NULL,
        regionID TEXT NOT NULL,
        state TEXT NOT NULL,
        createdAt DATETIME NOT NULL,
        firewallRanges JSON,
        expiresAt DATETIME,
        terminatedAt DATETIME,
        allowAllTraffic BOOL,
        updateWindow JSON
    );

    CREATE TABLE IF NOT EXISTS fusion.Workspaces (
        workspaceID TEXT PRIMARY KEY NOT NULL,
        name TEXT NOT NULL,
        workspaceGroupID TEXT NOT NULL,
        state TEXT NOT NULL,
        size TEXT NOT NULL,
        createdAt TEXT NOT NULL,
        endpoint TEXT,
        lastResumedAt TEXT,
        terminatedAt TEXT,
        scalingProgress INTEGER
    );

    CREATE TABLE IF NOT EXISTS fusion.FusionStatistics (
        tableName TEXT PRIMARY KEY NOT NULL,
        lastModifiedTime DATETIME NOT NULL
    );

    COMMIT;
'''
CACHE_TIMEOUTS = dict(
    Regions=datetime.timedelta(hours=12),
    WorkspaceGroups=datetime.timedelta(hours=1),
    Workspaces=datetime.timedelta(minutes=1),
)


def adapt_date_iso(val: Optional[datetime.date]) -> Optional[str]:
    """Adapt datetime.date to ISO 8601 date."""
    if val is None:
        return None
    return val.isoformat()


def adapt_datetime_iso(val: Optional[datetime.datetime]) -> Optional[str]:
    """Adapt datetime.datetime to timezone-naive ISO 8601 date."""
    if val is None:
        return None
    return val.isoformat()


def adapt_datetime_epoch(val: Optional[datetime.datetime]) -> Optional[int]:
    """Adapt datetime.datetime to Unix timestamp."""
    if val is None:
        return None
    return int(val.timestamp())


def adapt_json(val: Optional[Any]) -> Optional[str]:
    """Adapt JSON to TEXT."""
    if val is None:
        return None
    return json.dumps(val)


def adapt_bool(val: Optional[Any]) -> Optional[bool]:
    """Adapt any to bool."""
    if val is None:
        return None
    return bool(val)


sqlite3.register_adapter(datetime.date, adapt_date_iso)
sqlite3.register_adapter(datetime.datetime, adapt_datetime_iso)
sqlite3.register_adapter(bool, adapt_bool)
sqlite3.register_adapter(list, adapt_json)
sqlite3.register_adapter(dict, adapt_json)


def convert_date(val: Optional[bytes]) -> Optional[datetime.date]:
    """Convert ISO 8601 date to datetime.date object."""
    if val is None:
        return None
    return datetime.date.fromisoformat(val.decode())


def convert_datetime(val: Optional[bytes]) -> Optional[datetime.datetime]:
    """Convert ISO 8601 datetime to datetime.datetime object."""
    if val is None:
        return None
    return datetime.datetime.fromisoformat(val.decode())


def convert_timestamp(val: Optional[bytes]) -> Optional[datetime.datetime]:
    """Convert Unix epoch timestamp to datetime.datetime object."""
    if val is None:
        return None
    return datetime.datetime.fromtimestamp(int(val))


def convert_json(val: Optional[bytes]) -> Optional[Any]:
    """Convert JSON text to object."""
    if val is None:
        return None
    return json.loads(val)


sqlite3.register_converter('date', convert_date)
sqlite3.register_converter('datetime', convert_datetime)
sqlite3.register_converter('timestamp', convert_timestamp)
sqlite3.register_converter('json', convert_json)


def dict_factory(cursor: Any, row: Tuple[Any, ...]) -> Dict[str, Any]:
    """Return row as a dictionary."""
    return {k[0]: v for k, v in zip(cursor.description, row)}


class Cache(object):

    table_fields: Dict[str, List[str]] = {}

    def __init__(self) -> None:
        self.connection = self.connect()
        cur = self.connection.cursor()
        cur.executescript(SCHEMA)

        # Cache table fields
        if not self.table_fields:
            cur.execute('SELECT name FROM fusion.sqlite_master WHERE type = "table"')
            for table in cur.fetchall():
                info = cur.execute(f'PRAGMA fusion.table_info({table[0]})').fetchall()
                self.table_fields[table[0]] = info

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(':memory:', uri=True, detect_types=sqlite3.PARSE_DECLTYPES)
        conn.cursor().execute(f'ATTACH "{CACHE_NAME}" AS fusion')
        return conn

    def invalidate(self, table: str) -> None:
        with self.connect() as conn:
            cur = conn.cursor()
            cur.execute('DELETE FROM fusion.FusionStatistics WHERE tableName = "{table}"')

    def update(self, table: str, func: Callable[[], Any]) -> List[Dict[str, Any]]:
        with self.connect() as conn:

            cur = conn.cursor()

            stats = cur.execute(
                'SELECT tableName, lastModifiedTime FROM fusion.FusionStatistics '
                f'where tableName = "{table}"',
            ).fetchall()

            # If we are within the cache timeout, return the current results
            if stats and (datetime.datetime.now() - stats[0][1]) < CACHE_TIMEOUTS[table]:
                conn.row_factory = dict_factory
                return list(conn.cursor().execute(f'SELECT * FROM fusion.{table}'))

            # Build query components
            columns = [x[1] for x in self.table_fields[table]]
            values = func()
            fields = []
            field_subs = []
            conflict = []
            for k, v in values[0].items():
                if k not in columns:
                    continue
                fields.append(k)
                field_subs.append(f':{k}')
                conflict.append(f'{k}=excluded.{k}')

            try:
                cur.execute('BEGIN')

                # Insert the new value
                query = f'INSERT INTO fusion.{table}({", ".join(fields)}) ' \
                        f'  VALUES({", ".join(field_subs)}) ' \
                        f'  ON CONFLICT({columns[0]}) DO UPDATE SET {", ".join(conflict)}'
                cur.executemany(query, values)

                # Update the last modified time
                query = 'INSERT INTO fusion.FusionStatistics ' \
                        '  (tableName, lastModifiedTime) ' \
                        '  VALUES(:tableName, :lastModifiedTime) ' \
                        '  ON CONFLICT(tableName) DO UPDATE SET ' \
                        '      lastModifiedTime=excluded.lastModifiedTime'
                cur.execute(query, (table, datetime.datetime.now()))

                cur.execute('COMMIT')

            except Exception:
                cur.execute('ROLLBACK')

            return values
