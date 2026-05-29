"""In-process tests: validate recovery, seek-by-offset, unicode, 404, stats.

Run: python test_store.py   (with the venv active)
Uses a temp log file and re-instantiates EventStore to simulate a restart.
"""

import json
import os
import tempfile

from store import EventStore


def main() -> None:
    tmp = tempfile.mkdtemp()
    log_path = os.path.join(tmp, "events.log")

    # --- session 1: write events ---
    s1 = EventStore(log_path)
    assert s1.recovered_count == 0, "fresh store should recover 0"

    e1 = s1.append({"type": "signup", "user": "chidi"})
    e2 = s1.append({"type": "purchase", "amount": 42})
    e3 = s1.append({"type": "note", "text": "café — naïve 日本語"})  # unicode

    assert s1.stats()["total"] == 3
    print("session1 stats:", s1.stats())

    # read back within the same session
    assert s1.read(e1["id"])["user"] == "chidi"
    assert s1.read(e3["id"])["text"] == "café — naïve 日本語"
    assert s1.read("missing") is None

    # --- simulate restart: brand new instance over the same file ---
    s2 = EventStore(log_path)
    assert s2.recovered_count == 3, f"expected 3 recovered, got {s2.recovered_count}"
    print("session2 recovered:", s2.recovered_count, "stats:", s2.stats())

    # every previously written id is still readable after "restart"
    for ev in (e1, e2, e3):
        got = s2.read(ev["id"])
        assert got == ev, f"mismatch for {ev['id']}: {got} != {ev}"

    # unicode round-trips correctly via byte offsets
    assert s2.read(e3["id"])["text"] == "café — naïve 日本語"
    assert s2.read("missing") is None

    # bytes count is consistent with the actual file size
    assert s2.stats()["bytes"] == os.path.getsize(log_path), "byte count mismatch"

    # verify the read truly seeks (offset+length matches the line on disk)
    off, length = s2.index[e2["id"]]
    with open(log_path, "rb") as f:
        f.seek(off)
        raw = f.read(length)
    assert json.loads(raw)["id"] == e2["id"]

    # --- partial-write recovery: append a half-written line, ensure truncation ---
    with open(log_path, "ab") as f:
        f.write(b'{"id":"partial","oops"')  # no newline => crash mid-write
    s3 = EventStore(log_path)
    assert s3.recovered_count == 3, "partial line must not be counted"
    assert s3.read("partial") is None
    assert s3.stats()["bytes"] == os.path.getsize(log_path)
    print("session3 after partial-write recovery:", s3.stats())

    # a new append after recovery still works (log not corrupted)
    e4 = s3.append({"type": "after-crash"})
    assert s3.read(e4["id"])["type"] == "after-crash"

    print("\nALL EVENT STORE TESTS PASSED")


if __name__ == "__main__":
    main()
