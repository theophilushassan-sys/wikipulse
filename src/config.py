"""Central configuration, overridable through WIKIPULSE_* environment variables."""

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

# Storage layer (MongoDB)
MONGO_URI = os.environ.get("WIKIPULSE_MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.environ.get("WIKIPULSE_MONGO_DB", "wikipulse")
MONGO_COLLECTION = os.environ.get("WIKIPULSE_MONGO_COLLECTION", "events")

# Messaging layer (local SQLite-backed queue)
QUEUE_DB_PATH = os.environ.get("WIKIPULSE_QUEUE_DB", str(DATA_DIR / "queue.db"))

# Ingestion layer (Wikimedia EventStreams)
STREAM_URL = os.environ.get(
    "WIKIPULSE_STREAM_URL",
    "https://stream.wikimedia.org/v2/stream/recentchange",
)
USER_AGENT = os.environ.get(
    "WIKIPULSE_USER_AGENT",
    "WikiPulse/1.0 (real-time edit analytics; educational project)",
)
