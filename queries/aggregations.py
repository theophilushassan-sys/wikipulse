"""Analytics layer: near-real-time aggregations over the events collection.

Four queries over a sliding time window:
1. Trending pages (human edits to articles, namespace 0)
2. Most active users
3. Bot vs human activity split
4. Edit frequency per minute (spike detection view)
"""

import argparse
from datetime import datetime, timedelta, timezone

from pymongo import MongoClient

from src import config


def trending_pages(collection, since, limit=10):
    return list(collection.aggregate([
        {"$match": {"dt": {"$gte": since}, "is_bot": False, "namespace": 0}},
        {"$group": {
            "_id": {"title": "$title", "wiki": "$wiki"},
            "edits": {"$sum": 1},
            "editors": {"$addToSet": "$user"},
            "net_bytes": {"$sum": "$byte_change"},
        }},
        {"$project": {
            "edits": 1,
            "net_bytes": 1,
            "unique_editors": {"$size": "$editors"},
        }},
        {"$sort": {"edits": -1}},
        {"$limit": limit},
    ]))


def most_active_users(collection, since, limit=10):
    return list(collection.aggregate([
        {"$match": {"dt": {"$gte": since}}},
        {"$group": {
            "_id": "$user",
            "edits": {"$sum": 1},
            "is_bot": {"$first": "$is_bot"},
            "pages": {"$addToSet": "$title"},
        }},
        {"$project": {"edits": 1, "is_bot": 1, "unique_pages": {"$size": "$pages"}}},
        {"$sort": {"edits": -1}},
        {"$limit": limit},
    ]))


def bot_vs_human(collection, since):
    return list(collection.aggregate([
        {"$match": {"dt": {"$gte": since}}},
        {"$group": {"_id": "$is_bot", "edits": {"$sum": 1}}},
        {"$sort": {"_id": 1}},
    ]))


def edits_per_minute(collection, since, last_n=10):
    return list(collection.aggregate([
        {"$match": {"dt": {"$gte": since}}},
        {"$group": {
            "_id": {"$dateTrunc": {"date": "$dt", "unit": "minute"}},
            "edits": {"$sum": 1},
        }},
        {"$sort": {"_id": -1}},
        {"$limit": last_n},
        {"$sort": {"_id": 1}},
    ]))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--minutes", type=int, default=60,
                        help="size of the analysis window in minutes")
    args = parser.parse_args()

    client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=5000)
    collection = client[config.MONGO_DB][config.MONGO_COLLECTION]
    since = datetime.now(timezone.utc) - timedelta(minutes=args.minutes)
    total = collection.count_documents({"dt": {"$gte": since}})

    print(f"=== WikiPulse analytics: last {args.minutes} minutes "
          f"(since {since:%Y-%m-%d %H:%M:%S} UTC) ===")
    print(f"total events in window: {total}\n")

    print("--- Trending pages (human article edits) ---")
    for i, row in enumerate(trending_pages(collection, since), 1):
        print(f"{i:>2}. {row['_id']['title']} [{row['_id']['wiki']}]  "
              f"edits={row['edits']}  editors={row['unique_editors']}  "
              f"net_bytes={row['net_bytes']:+d}")

    print("\n--- Most active users ---")
    for i, row in enumerate(most_active_users(collection, since), 1):
        kind = "bot" if row["is_bot"] else "human"
        print(f"{i:>2}. {row['_id']} ({kind})  edits={row['edits']}  "
              f"pages={row['unique_pages']}")

    print("\n--- Bot vs human activity ---")
    split = bot_vs_human(collection, since)
    window_total = sum(row["edits"] for row in split) or 1
    for row in split:
        label = "bots" if row["_id"] else "humans"
        share = 100.0 * row["edits"] / window_total
        print(f"{label:>7}: {row['edits']} edits ({share:.1f}%)")

    print("\n--- Edits per minute (most recent buckets) ---")
    for row in edits_per_minute(collection, since):
        print(f"{row['_id']:%H:%M} UTC  {row['edits']}")

    client.close()


if __name__ == "__main__":
    main()
