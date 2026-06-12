"""SQLite-backed local message queue.

This is the messaging/decoupling layer between the ingestion producer and the
stream processor. Each component opens its own QueueStore against the same
database file; SQLite's WAL mode allows one writer and concurrent readers, so
the producer and consumer can run (and crash/restart) independently without
losing messages. Messages are removed only after an explicit ack, giving
at-least-once delivery; the unique event_id index in MongoDB makes the overall
pipeline effectively exactly-once.
"""

import json
import os
import sqlite3
import threading
import time


class QueueStore:
    def __init__(self, db_path: str):
        directory = os.path.dirname(db_path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        self._conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                payload TEXT NOT NULL,
                enqueued_at REAL NOT NULL
            )
            """
        )
        self._conn.commit()
        self._lock = threading.Lock()

    def put(self, event: dict) -> None:
        """Append one event (any JSON-serializable dict) to the queue."""
        with self._lock:
            self._conn.execute(
                "INSERT INTO queue (payload, enqueued_at) VALUES (?, ?)",
                (json.dumps(event), time.time()),
            )
            self._conn.commit()

    def get_batch(self, limit: int = 100) -> list:
        """Return up to `limit` oldest messages as (message_id, event) pairs.

        Messages stay in the queue until ack()ed, so a consumer crash between
        get_batch and ack only causes redelivery, never loss.
        """
        cursor = self._conn.execute(
            "SELECT id, payload FROM queue ORDER BY id LIMIT ?", (limit,)
        )
        return [(row[0], json.loads(row[1])) for row in cursor.fetchall()]

    def ack(self, message_ids) -> None:
        """Delete processed messages from the queue."""
        ids = list(message_ids)
        if not ids:
            return
        placeholders = ",".join("?" * len(ids))
        with self._lock:
            self._conn.execute(f"DELETE FROM queue WHERE id IN ({placeholders})", ids)
            self._conn.commit()

    def size(self) -> int:
        return self._conn.execute("SELECT COUNT(*) FROM queue").fetchone()[0]

    def close(self) -> None:
        self._conn.close()
