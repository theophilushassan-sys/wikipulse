from src.queue_store import QueueStore


def make_queue(tmp_path):
    return QueueStore(str(tmp_path / "queue.db"))


def test_put_and_get_batch_roundtrip(tmp_path):
    queue = make_queue(tmp_path)
    queue.put({"n": 1})
    queue.put({"n": 2})
    batch = queue.get_batch(10)
    assert [event for _, event in batch] == [{"n": 1}, {"n": 2}]
    assert queue.size() == 2  # unacked messages stay queued
    queue.close()


def test_ack_removes_messages(tmp_path):
    queue = make_queue(tmp_path)
    for n in range(5):
        queue.put({"n": n})
    batch = queue.get_batch(3)
    queue.ack([message_id for message_id, _ in batch])
    assert queue.size() == 2
    remaining = [event["n"] for _, event in queue.get_batch(10)]
    assert remaining == [3, 4]
    queue.close()


def test_fifo_order_across_batches(tmp_path):
    queue = make_queue(tmp_path)
    for n in range(6):
        queue.put({"n": n})
    seen = []
    while queue.size():
        batch = queue.get_batch(2)
        seen.extend(event["n"] for _, event in batch)
        queue.ack([message_id for message_id, _ in batch])
    assert seen == [0, 1, 2, 3, 4, 5]
    queue.close()
