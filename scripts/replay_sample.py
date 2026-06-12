"""Offline smoke test: enqueue synthetic recentchange events (no network needed).

Generates --count realistic raw events spread over the last ~50 minutes and
publishes them to the local queue exactly as src.ingest would. Exactly one
event is enqueued twice (same meta.id), so after the processor drains the
queue MongoDB must contain count-1 documents and report 1 duplicate absorbed.
"""

import argparse
import random
import time
import uuid

from src import config
from src.queue_store import QueueStore

PAGES = [
    ("Artificial intelligence", "en.wikipedia.org", "enwiki"),
    ("2026 FIFA World Cup", "en.wikipedia.org", "enwiki"),
    ("Climate change", "en.wikipedia.org", "enwiki"),
    ("Python (programming language)", "en.wikipedia.org", "enwiki"),
    ("Elon Musk", "en.wikipedia.org", "enwiki"),
    ("Taylor Swift", "en.wikipedia.org", "enwiki"),
    ("World Health Organization", "en.wikipedia.org", "enwiki"),
    ("Olympic Games", "en.wikipedia.org", "enwiki"),
    ("Quantum computing", "en.wikipedia.org", "enwiki"),
    ("Mbappé", "fr.wikipedia.org", "frwiki"),
    ("Bundestag", "de.wikipedia.org", "dewiki"),
    ("Inteligencia artificial", "es.wikipedia.org", "eswiki"),
]

HUMANS = ["WikiGnome42", "EditorJane", "HistoryBuff77", "Cartographer",
          "Semyon K.", "Lucia M", "DataDruid", "Anon1928"]
BOTS = ["SuccessionBot", "CitationCleanerBot", "InternetArchiveBot", "AnomieBOT"]


def make_event(rng: random.Random, now: float) -> dict:
    title, domain, wiki = rng.choice(PAGES)
    is_bot = rng.random() < 0.35
    user = rng.choice(BOTS if is_bot else HUMANS)
    old_len = rng.randint(500, 80000)
    timestamp = int(now - rng.uniform(0, 50 * 60))
    return {
        "meta": {"id": str(uuid.uuid4()), "domain": domain},
        "type": "edit" if rng.random() < 0.9 else "new",
        "title": title,
        "user": user,
        "bot": is_bot,
        "minor": rng.random() < 0.2,
        "wiki": wiki,
        "server_name": domain,
        "namespace": 0,
        "comment": "synthetic replay event",
        "timestamp": timestamp,
        "length": {"old": old_len, "new": old_len + rng.randint(-400, 900)},
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=500,
                        help="total events to enqueue, including 1 duplicate")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    if args.count < 2:
        parser.error("--count must be at least 2 to include a duplicate")

    rng = random.Random(args.seed)
    now = time.time()
    queue = QueueStore(config.QUEUE_DB_PATH)

    events = [make_event(rng, now) for _ in range(args.count - 1)]
    duplicate = rng.choice(events)
    events.append(duplicate)
    rng.shuffle(events)

    for event in events:
        queue.put(event)

    print(f"enqueued {len(events)} events "
          f"({args.count - 1} unique, 1 intentional duplicate)")
    print(f"queue size now: {queue.size()} at {config.QUEUE_DB_PATH}")
    queue.close()


if __name__ == "__main__":
    main()
