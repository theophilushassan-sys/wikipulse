"""Create the MongoDB indexes that back WikiPulse's query patterns.

- event_id (unique): deduplication guarantee for the processor
- dt: time-window queries
- user + dt: per-user activity
- title + dt: per-page activity / trending
- is_bot + dt: bot vs human splits over time
"""

from pymongo import ASCENDING, DESCENDING, MongoClient

from src import config

INDEXES = [
    ([("event_id", ASCENDING)], {"name": "event_id_unique", "unique": True}),
    ([("dt", DESCENDING)], {"name": "dt_desc"}),
    ([("user", ASCENDING), ("dt", DESCENDING)], {"name": "user_dt"}),
    ([("title", ASCENDING), ("dt", DESCENDING)], {"name": "title_dt"}),
    ([("is_bot", ASCENDING), ("dt", DESCENDING)], {"name": "is_bot_dt"}),
]


def main() -> None:
    client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=5000)
    collection = client[config.MONGO_DB][config.MONGO_COLLECTION]
    for keys, options in INDEXES:
        name = collection.create_index(keys, **options)
        print(f"created index: {name}")
    existing = [
        index["name"] for index in collection.list_indexes() if index["name"] != "_id_"
    ]
    print(f"\n{len(existing)} WikiPulse indexes present on "
          f"{config.MONGO_DB}.{config.MONGO_COLLECTION}: {', '.join(existing)}")
    client.close()


if __name__ == "__main__":
    main()
