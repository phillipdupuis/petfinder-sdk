import os
import sqlite3
import time
from types import TracebackType
from typing import Type, Union, NamedTuple

from petfinder.caching.core import (
    RequestsCache,
    TimeToLiveCallback,
    default_ttl_callback,
    data_directory,
)


class Row(NamedTuple):
    key: bytes
    timestamp: float
    response: bytes


_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS requests (
    key blob PRIMARY KEY,
	timestamp real NOT NULL,
	response blob NOT NULL
);
"""


class SqliteCache(RequestsCache):
    db_file: str
    conn: Union[sqlite3.Connection, None]

    def __init__(
        self,
        db_file: str = None,
        time_to_live: TimeToLiveCallback = default_ttl_callback,
    ) -> None:
        super().__init__(time_to_live)
        self.db_file = db_file or os.path.join(data_directory(), "sqlite.db")
        self.conn = None
        with Cursor(self) as c:
            c.execute(_CREATE_TABLE)
        # self.clean()

    def open(self):
        self.conn = sqlite3.connect(self.db_file)

    def is_open(self) -> bool:
        return self.conn is not None

    def close(self):
        if self.conn.in_transaction:
            self.conn.commit()
        self.conn.close()
        self.conn = None

    def clean(self):
        with Cursor(self) as c:
            c.execute("SELECT key FROM requests")
            for row in c.fetchall():
                key = row[0]
                if self.is_expired(key):
                    c.execute("DELETE FROM requests WHERE key = ?", (key,))

    def __del__(self):
        pass

    def __getitem__(self, key: bytes) -> bytes:
        with Cursor(self) as c:
            c.execute("SELECT response FROM requests WHERE key = ? LIMIT 1", (key,))
            row = c.fetchone()
            if row is None:
                raise KeyError(f"{key} is not in {self}")
            return row[0]

    def __setitem__(self, key: bytes, value: bytes):
        with Cursor(self) as c:
            c.execute(
                "INSERT INTO requests (key, timestamp, response) VALUES (?, ?, ?)",
                (key, time.time(), value),
            )

    def __delitem__(self, key: bytes):
        with Cursor(self) as c:
            c.execute("DELETE FROM requests WHERE key = ?", (key,))

    def __contains__(self, key: bytes) -> bool:
        with Cursor(self) as c:
            c.execute("SELECT 1 FROM requests WHERE key = ? LIMIT 1", (key,))
            return bool(c.fetchone())

    def get_timestamp(self, key: bytes) -> int:
        with Cursor(self) as c:
            c.execute("SELECT timestamp FROM requests WHERE key = ? LIMIT 1", (key,))
            return c.fetchone()[0]


class Cursor:
    cache: SqliteCache
    close_on_exit: bool

    def __init__(self, cache: SqliteCache):
        self.cache = cache
        self.close_on_exit = False

    def __enter__(self) -> sqlite3.Cursor:
        if not self.cache.is_open():
            self.cache.open()
            self.close_on_exit = True
        return self.cache.conn.cursor()

    def __exit__(
        self,
        exc_type: Type[BaseException] = None,
        exc_val: BaseException = None,
        exc_tb: TracebackType = None,
    ) -> None:
        if self.close_on_exit:
            self.cache.close()
