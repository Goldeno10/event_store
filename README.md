# Append-Only Event Store

A minimal key–value event store where **the log file *is* the database**. Events
are written as one JSON object per line to `events.log` (strictly append-only),
and an in-memory index maps each event id to the exact byte range of its line so
reads seek directly to the bytes instead of scanning the file. On startup the log
is replayed to rebuild the index — the same trick Postgres, SQLite and Kafka use
to survive crashes.

- **Stack:** Python 3.12 + FastAPI + Uvicorn. No SQLite, no JSON rewrites — just an append-only file and a `dict`.

---

## Setup

```bash
cd event_store
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# start the server (events.log is created next to it)
uvicorn main:app --reload --port 8000
```

The log location can be overridden: `EVENTS_LOG=/path/to/events.log uvicorn main:app`.

### Endpoints (curl)

```bash
# POST /events  -> stamps {id, createdAt}, appends a line, returns 201 + full event
curl -s -X POST http://127.0.0.1:8000/events \
  -H 'Content-Type: application/json' \
  -d '{"type":"signup","user":"chidi"}'
# {"type":"signup","user":"chidi","id":"7febbce5-...","createdAt":"2026-05-29T08:..."}

# GET /events/:id  -> seeks via the index and returns the event (404 if unknown)
curl -s http://127.0.0.1:8000/events/7febbce5-b2ab-44b9-ab36-390b421db548

# GET /stats  -> {"total": <#events>, "bytes": <log size in bytes>}
curl -s http://127.0.0.1:8000/stats
```

### One-command restart demo

```bash
./demo.sh   # writes 3 events -> stops -> restarts -> reads still work -> shows recovery count
```

---

## Architecture

```
                   POST /events                         GET /events/:id
                        │                                     │
                        ▼                                     ▼
              ┌───────────────────┐                 ┌───────────────────┐
              │ stamp id+createdAt │                 │ index.get(id)     │
              │ json + "\n"        │                 │  -> (offset, len) │
              └─────────┬─────────┘                 └─────────┬─────────┘
                        │ append (O_APPEND) + fsync           │ seek(offset)
                        ▼                                     │ read(len)
   events.log ──────────────────────────────────────────────┼────────────►
   (append-only, one JSON object per line)                   │
   line 0  {"id":"a",...}\n   offset=0   len=24              │ parse JSON
   line 1  {"id":"b",...}\n   offset=25  len=31  ◄───────────┘ return
   line 2  {"id":"c",...}\n   offset=57  len=40

              ┌─────────────────────────────────────────┐
              │ In-memory index  Map<id, {offset, len}>  │
              │  a -> (0, 24)   b -> (25, 31)  ...        │  (rebuilt on startup
              └─────────────────────────────────────────┘   by replaying the log)
```

**Write path:** stamp `id` + `createdAt` → serialize to one UTF-8 JSON line →
append to the file (`O_APPEND`) → `fsync` so it's durable → record
`id -> (offset, length)` in the index.

**Read path:** look up `id` in the index → `seek(offset)` → `read(length)` →
parse. No scan, O(1) lookup regardless of file size.

**Recovery:** on startup the log is streamed line by line, byte offsets are
tracked, and the index is rebuilt. A trailing partial line (a crash mid-write) is
detected and truncated so the next append stays clean.

---

## Core concepts, in my own words

**Why append-only is safer than overwriting in place.** When you overwrite data
in place and the process (or the machine) dies halfway, you can end up with a
record that is *neither* the old value nor the new one — silent corruption.
Appending never touches existing bytes: the worst a crash can do is leave a
half-written line *at the very end*, which is trivially detectable (no trailing
`\n`) and recoverable (truncate it). Old data is always intact. That's why real
databases write to a write-ahead log first and only then apply changes.

**Why an index makes reads fast.** Without an index, finding an event means
scanning the whole file — O(n) and slower as the log grows. The index stores the
exact `(offset, length)` of every record, so a read is one hash lookup plus a
single `seek`+`read` of just those bytes: O(1) and independent of file size. The
index is pure derived state — if it's lost, we rebuild it from the log.

**The unicode gotcha.** Offsets and lengths must be measured in **bytes**, not
characters. `"café 日本語"` is 9 characters but more bytes in UTF-8. The code
serializes with `ensure_ascii=False`, encodes to UTF-8, records `len(bytes)`, and
seeks/reads in binary mode — so multi-byte payloads round-trip correctly.

---

## Recovery log after a restart

Starting a fresh process against an existing `events.log` replays it and logs the
rebuilt count:

```
2026-05-29 09:55:38,072 INFO Recovery complete: rebuilt index for 2 events from events.log (254 bytes)
```

`GET /stats` immediately after confirms the same numbers:

```json
{"total": 2, "bytes": 254}
```

> Replace this block with a screenshot of your own terminal for submission.

---

## What I struggled with

- **Byte vs character offsets.** My first version tracked offsets with string
  lengths and `seek` on a text-mode file. It worked until I added a unicode
  payload, then reads returned truncated/garbled JSON. Fix: do everything in
  binary mode and count UTF-8 bytes.
- **Where the newline lives.** I had to decide whether `length` includes the
  trailing `\n`. I store the JSON bytes only (newline excluded) so a read parses
  cleanly without stripping. Offsets still advance by `len(line) + 1`.
- **Partial trailing lines.** Simulating a crash by appending a half-written line
  made recovery count it as a real event. I added detection (no trailing `\n`) +
  truncation so the store self-heals on startup.
- **Blocking the event loop.** `fsync` is blocking; calling it directly inside an
  `async` endpoint stalls the server under load. I moved the file I/O onto a
  threadpool with `run_in_threadpool`.

## What I learned

- How write-ahead logs and log-structured storage actually work under the hood,
  and why "append + index + replay" is the backbone of durable systems.
- The difference between bytes and characters in file I/O, and why binary mode
  matters for correctness with unicode.
- `fsync` and the durability guarantee (and its cost), plus offloading blocking
  I/O from the async event loop in FastAPI/Starlette.
- FastAPI lifespan handlers for running startup logic (recovery) once.

## Resources I consulted

- Designing Data-Intensive Applications, Ch. 3 (log-structured storage, indexes) — Martin Kleppmann
- FastAPI docs — lifespan events: https://fastapi.tiangolo.com/advanced/events/
- Python docs — `io` and binary file objects: https://docs.python.org/3/library/io.html
- Python docs — `os.fsync`: https://docs.python.org/3/library/os.html#os.fsync
- "How does a database store data on disk" — various blog posts on WAL / LSM trees

## Why this made me a better backend developer

I can now reason about durability from first principles instead of treating "the
database" as a black box: I understand *why* append-only + fsync survives crashes,
*why* an index turns an O(n) scan into an O(1) read, and *why* recovery is just a
replay of the log. Concretely, I can build a crash-safe write path, debug
corruption issues by reasoning about byte offsets, and explain the trade-off
between durability (fsync on every write) and throughput. In production I'll now
think harder about partial writes, what happens on restart, and whether my reads
scale with data size — the kinds of things that turn a minor outage into a major
one if you get them wrong.

---

## Tests

```bash
python test_store.py
```

Covers: fresh start, writes + reads, unicode round-trip, restart recovery (a new
`EventStore` over the same file), 404 for unknown ids, byte-count consistency,
direct seek correctness, and partial-write crash recovery.
