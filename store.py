"""Append-only event store.

The log file *is* the database. Each event is one JSON object per line,
appended (never overwritten). An in-memory index maps an event id to the
exact byte range of its line so reads seek directly instead of scanning.
"""

from __future__ import annotations

import json
import os
import threading
import uuid
from datetime import datetime, timezone
from typing import Optional


class EventStore:
    def __init__(self, log_path: str):
        self.log_path = log_path
        # id -> (offset_in_bytes, length_in_bytes)  length excludes the "\n"
        self.index: dict[str, tuple[int, int]] = {} # index is a dictionary that maps event ids to their byte offsets and lengths in bytes eg {"123": (100, 10)}
        self.bytes_written = 0
        self.recovered_count = 0
        self._lock = threading.Lock()
        self.recovered_count = self._recover()

    def _recover(self) -> int:
        """Stream the log on startup and rebuild the index from byte offsets.

        Tracks offsets in bytes (not characters) so unicode payloads are
        handled correctly. A trailing partial line (a crash mid-write) is
        detected and truncated so the next append stays clean.
        """
        if not os.path.exists(self.log_path):
            # Touch the file so append mode has something to open later.
            open(self.log_path, "ab").close()
            self.bytes_written = 0
            return 0

        count = 0
        offset = 0
        valid_end = 0
        with open(self.log_path, "rb") as f: # open the log file in binary mode
            for raw in f:  # binary iteration yields one "\n"-terminated chunk
                if raw.endswith(b"\n"):
                    line = raw[:-1] # remove the trailing newline
                    if line.strip(): # if the line is not empty
                        try:
                            obj = json.loads(line)
                            self.index[obj["id"]] = (offset, len(line)) # add the event id to the index with the byte offset and length
                            count += 1
                        except (json.JSONDecodeError, KeyError):
                            # Skip a malformed line but keep its bytes counted.
                            pass
                    offset += len(raw)
                    valid_end = offset
                else:
                    # No trailing newline => partial write from a crash.
                    offset += len(raw)
                    break

        if valid_end < offset:
            # Drop the partial trailing line so appends resume cleanly.
            with open(self.log_path, "r+b") as f:
                f.truncate(valid_end)

        self.bytes_written = valid_end
        return count

    def append(self, body: dict) -> dict:
        """Stamp id + createdAt, append one JSON line, update the index."""
        event = dict(body)
        event["id"] = str(uuid.uuid4())
        event["createdAt"] = datetime.now(timezone.utc).isoformat()

        # ensure_ascii=False keeps unicode as real UTF-8 bytes so the byte
        # length we record matches what lands on disk.
        encoded = json.dumps(event, ensure_ascii=False).encode("utf-8")

        with self._lock:
            offset = self.bytes_written
            with open(self.log_path, "ab") as f:
                f.write(encoded + b"\n")
                f.flush()
                os.fsync(f.fileno())  # durable before we ack the write
            self.index[event["id"]] = (offset, len(encoded))
            self.bytes_written += len(encoded) + 1  # +1 for the newline

        return event

    def read(self, event_id: str) -> Optional[dict]:
        """Seek directly to the event's byte range using the index."""
        entry = self.index.get(event_id)
        if entry is None:
            return None
        offset, length = entry
        with open(self.log_path, "rb") as f:
            f.seek(offset)
            raw = f.read(length)
        return json.loads(raw)

    def stats(self) -> dict:
        return {"total": len(self.index), "bytes": self.bytes_written}
