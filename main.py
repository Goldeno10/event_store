"""FastAPI app exposing the append-only event store."""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from starlette.concurrency import run_in_threadpool

from store import EventStore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("event_store")

LOG_PATH = os.environ.get("EVENTS_LOG", "events.log")

# Construct (and recover) the store at import time so endpoints can use it.
store = EventStore(LOG_PATH)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(
        "Recovery complete: rebuilt index for %d events from %s (%d bytes)",
        store.recovered_count,
        LOG_PATH,
        store.bytes_written,
    )
    yield


app = FastAPI(title="Append-Only Event Store", lifespan=lifespan)


@app.post("/events", status_code=201)
async def create_event(request: Request):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Request body must be valid JSON")
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Request body must be a JSON object")
    # File I/O + fsync is blocking; run it off the event loop.
    return await run_in_threadpool(store.append, body)


@app.get("/events/{event_id}")
async def get_event(event_id: str):
    event = await run_in_threadpool(store.read, event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="Event not found")
    return event


@app.get("/stats")
async def get_stats():
    return store.stats()
