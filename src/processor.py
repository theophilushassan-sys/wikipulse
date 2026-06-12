"""Stream processing layer: SQLite queue -> transform -> MongoDB.

Consumes queued raw events in batches, standardizes them, and inserts them
into MongoDB. Deduplication relies on the unique event_id index: duplicate
inserts fail with error code 11000 and are counted as absorbed, never stored
twice. Messages are acked (deleted from the queue) only after the batch has
been handed to MongoDB.
"""

import argparse
import logging
import time

from pymongo import MongoClient
from pymongo.errors import BulkWriteError

from . import config
from .queue_store import QueueStore
from .transform import transform_event

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [processor] %(message)s",
)
log = logging.getLogger(__name__)

BATCH_SIZE = 200
IDLE_SLEEP_SECONDS = 1.0
DUPLICATE_KEY_ERROR = 11000


def process_batch(collection, batch):
    """Transform and insert one batch. Returns (inserted, duplicates, invalid)."""
    documents = []
    invalid = 0
    for _message_id, raw in batch:
        document = transform_event(raw)
        if document is None:
            invalid += 1
        else:
            documents.append(document)

    inserted = duplicates = 0
    if documents:
        try:
            result = collection.insert_many(documents, ordered=False)
            inserted = len(result.inserted_ids)
        except BulkWriteError as exc:
            write_errors = exc.details.get("writeErrors", [])
            unexpected = [
                e for e in write_errors if e.get("code") != DUPLICATE_KEY_ERROR
            ]
            if unexpected:
                raise
            duplicates = len(write_errors)
            inserted = exc.details.get("nInserted", 0)
    return inserted, duplicates, invalid


def run(drain: bool = False) -> None:
    queue = QueueStore(config.QUEUE_DB_PATH)
    client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=5000)
    collection = client[config.MONGO_DB][config.MONGO_COLLECTION]

    totals = {"inserted": 0, "duplicates": 0, "invalid": 0}
    log.info("consuming queue=%s -> %s.%s", config.QUEUE_DB_PATH,
             config.MONGO_DB, config.MONGO_COLLECTION)
    try:
        while True:
            batch = queue.get_batch(BATCH_SIZE)
            if not batch:
                if drain:
                    break
                time.sleep(IDLE_SLEEP_SECONDS)
                continue
            inserted, duplicates, invalid = process_batch(collection, batch)
            queue.ack([message_id for message_id, _ in batch])
            totals["inserted"] += inserted
            totals["duplicates"] += duplicates
            totals["invalid"] += invalid
            log.info(
                "batch=%d inserted=%d duplicates=%d invalid=%d queue_remaining=%d",
                len(batch), inserted, duplicates, invalid, queue.size(),
            )
    except KeyboardInterrupt:
        pass
    finally:
        log.info(
            "done: inserted=%d duplicates_absorbed=%d invalid=%d",
            totals["inserted"], totals["duplicates"], totals["invalid"],
        )
        queue.close()
        client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--drain",
        action="store_true",
        help="exit once the queue is empty instead of waiting for new events",
    )
    args = parser.parse_args()
    run(drain=args.drain)
