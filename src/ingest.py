"""Ingestion layer: Wikimedia EventStreams (SSE) -> local SQLite queue.

Connects to the recentchange stream, validates/parses each JSON event and
publishes it to the queue. Reconnects with exponential backoff on network
errors so it can run unattended.
"""

import json
import logging
import time

import requests
import sseclient

from . import config
from .queue_store import QueueStore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [ingest] %(message)s",
)
log = logging.getLogger(__name__)

ACCEPTED_TYPES = {"edit", "new"}
LOG_EVERY = 250


def run() -> None:
    queue = QueueStore(config.QUEUE_DB_PATH)
    headers = {"User-Agent": config.USER_AGENT, "Accept": "text/event-stream"}
    backoff = 1
    received = 0
    log.info("connecting to %s", config.STREAM_URL)
    while True:
        try:
            response = requests.get(
                config.STREAM_URL, stream=True, headers=headers, timeout=(10, 60)
            )
            response.raise_for_status()
            client = sseclient.SSEClient(response)
            backoff = 1
            for sse_event in client.events():
                if not sse_event.data:
                    continue
                try:
                    raw = json.loads(sse_event.data)
                except json.JSONDecodeError:
                    continue
                if not isinstance(raw, dict) or raw.get("type") not in ACCEPTED_TYPES:
                    continue
                queue.put(raw)
                received += 1
                if received % LOG_EVERY == 0:
                    log.info("received=%d queued=%d", received, queue.size())
        except KeyboardInterrupt:
            log.info("stopping; received=%d total", received)
            break
        except requests.RequestException as exc:
            log.warning("stream error (%s); reconnecting in %ds", exc, backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)
    queue.close()


if __name__ == "__main__":
    run()
