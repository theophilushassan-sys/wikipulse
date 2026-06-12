from datetime import timezone

from src.transform import transform_event


def full_raw_event():
    return {
        "meta": {"id": "abc-123", "domain": "en.wikipedia.org"},
        "type": "edit",
        "title": "Artificial intelligence",
        "user": "EditorJane",
        "bot": False,
        "minor": False,
        "wiki": "enwiki",
        "server_name": "en.wikipedia.org",
        "namespace": 0,
        "comment": "fix typo",
        "timestamp": 1750000000,
        "length": {"old": 1000, "new": 1120},
    }


def test_transform_standard_event():
    doc = transform_event(full_raw_event())
    assert doc["event_id"] == "abc-123"
    assert doc["title"] == "Artificial intelligence"
    assert doc["user"] == "EditorJane"
    assert doc["is_bot"] is False
    assert doc["byte_change"] == 120
    assert doc["dt"].tzinfo == timezone.utc


def test_transform_fills_missing_optional_fields():
    doc = transform_event({"meta": {"id": "only-id"}})
    assert doc["event_id"] == "only-id"
    assert doc["title"] == "(unknown)"
    assert doc["user"] == "(unknown)"
    assert doc["byte_change"] == 0
    assert doc["comment"] == ""
    assert doc["dt"] is not None


def test_transform_rejects_event_without_id():
    assert transform_event({}) is None
    assert transform_event({"meta": {}, "title": "No id"}) is None


def test_transform_bot_flag():
    raw = full_raw_event()
    raw["bot"] = True
    raw["user"] = "AnomieBOT"
    assert transform_event(raw)["is_bot"] is True
