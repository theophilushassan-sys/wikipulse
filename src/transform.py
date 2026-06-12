"""Cleaning, schema standardization and enrichment for raw recentchange events."""

from datetime import datetime, timezone


def transform_event(raw: dict):
    """Convert a raw Wikimedia recentchange event into the standard document schema.

    Returns None when the event is unusable (no unique event id to dedupe on).
    Missing optional fields are filled with safe defaults so downstream
    aggregations never hit absent keys.
    """
    meta = raw.get("meta") or {}
    event_id = meta.get("id")
    if not event_id:
        return None

    timestamp = raw.get("timestamp")
    if timestamp is not None:
        dt = datetime.fromtimestamp(int(timestamp), tz=timezone.utc)
    elif meta.get("dt"):
        dt = datetime.fromisoformat(str(meta["dt"]).replace("Z", "+00:00"))
    else:
        dt = datetime.now(timezone.utc)

    length = raw.get("length") or {}
    old_len = int(length.get("old") or 0)
    new_len = int(length.get("new") or 0)

    return {
        "event_id": event_id,
        "type": raw.get("type") or "unknown",
        "title": raw.get("title") or "(unknown)",
        "user": raw.get("user") or "(unknown)",
        "is_bot": bool(raw.get("bot", False)),
        "minor": bool(raw.get("minor", False)),
        "wiki": raw.get("wiki") or meta.get("domain") or "(unknown)",
        "server_name": raw.get("server_name") or meta.get("domain") or "(unknown)",
        "namespace": int(raw.get("namespace") or 0),
        "comment": raw.get("comment") or "",
        "dt": dt,
        "byte_change": new_len - old_len,
    }
