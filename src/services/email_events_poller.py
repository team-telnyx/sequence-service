"""Email API events pull-poller (REVOPS-1525 pivot from webhook push).

The Ed25519 webhook receiver (src/api/email_events.py, PR #30) is live but
Telnyx cannot reach the host — no public ingress (corporate tailnet, Funnel
not permitted). This module PULLS delivery events from GET /v2/email/events
on a ~2-minute interval and feeds them through the SAME process_email_event
the receiver uses, so bounce/complaint/unsubscribe/delivered handling is
identical between the push and pull paths. The receiver stays as-is for a
possible future push path.

API behavior (probed live 2026-08-17 — trust this over docs):
  - GET /v2/email/events, auth Bearer $EMAIL_API_KEY
  - Returns oldest-first pages of 25: {data: [{id, event_type, occurred_at,
    payload: {id, status, from, to, subject, ...}}], meta: {page_size,
    page_cursor}}
  - Pagination: page[cursor]=<meta.page_cursor> for the next page; cursor
    is base64 of occurred_at|event_id; advances chronologically.
  - TRAPS (verified): filter[occurred_at][gte] is SILENTLY IGNORED;
    filter[event_type] returns 0 rows (broken). Do NOT rely on server-side
    filtering — pull everything and filter client-side by event_type.

Cursor semantics:
  - On empty/missing cursor, start from the first page (ProcessedEmailEvent
    dedupe markers make the backfill safe — re-pulled events are no-ops).
  - The cursor advances ONLY after every event on a page is processed
    successfully. A failed page leaves the cursor untouched so the next run
    re-fetches and retries (already-processed events no-op via the marker).
  - A short page (< page_size) ends the run.

Failure behavior:
  - API unreachable/401/5xx → log warning, leave cursor untouched, return.
    poll_once never raises — the launchd runner wraps it without try/except
    for control flow (the runner still has its own catch-all to protect the
    host process).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import httpx
import structlog
from sqlalchemy.ext.asyncio import async_sessionmaker

from src.models.models import EmailEventsPollerCursor
from src.services.email_events import (
    EVENT_TYPE_MAP,
    EmailEvent,
    process_email_event,
)

logger = structlog.get_logger()

EVENTS_PATH = "/email/events"
POLLER_FEED = "email_events"
DEFAULT_PAGE_SIZE = 25

# Internal event types the poller OWNS. queued/sending/sent are owned by the
# send path (the poller must not double-count send-state); opened/clicked are
# engagement events out of scope. Items outside this set are skipped
# client-side (the API's filter[event_type] is broken — verified 2026-08-17).
_HANDLED_INTERNAL_TYPES = frozenset(
    {"delivered", "bounce", "complaint", "unsubscribe", "suppressed"}
)


class PollerAPIError(Exception):
    """Non-2xx response from GET /v2/email/events. Carries the status code."""

    def __init__(self, message: str, status_code: int) -> None:
        super().__init__(message)
        self.status_code = status_code


@dataclass
class PollSummary:
    pages: int = 0
    processed: int = 0
    already_processed: int = 0
    unmatched: int = 0
    skipped: int = 0
    errors: int = 0
    cursor_advanced: bool = False


def extract_event_from_api_item(item: dict) -> Optional[EmailEvent]:
    """Parse one /v2/email/events ``data`` list item into an EmailEvent.

    Returns None for event types the poller does not own (queued/sending/sent
    are send-path owned; opened/clicked are engagement, out of scope) and for
    unknown event types.

    Raises KeyError if a required field (event_type, id, payload.id) is
    missing — the caller catches, logs, and skips the item without crashing
    the run.
    """
    raw_type = item["event_type"]
    internal_type = EVENT_TYPE_MAP.get(raw_type)
    if internal_type is None or internal_type not in _HANDLED_INTERNAL_TYPES:
        return None
    event_id = item["id"]
    occurred_at = item.get("occurred_at")
    payload = item["payload"]
    message_id = payload["id"]
    to_raw = payload.get("to")
    if isinstance(to_raw, list):
        to_email = to_raw[0] if to_raw else ""
    else:
        to_email = to_raw or ""
    return EmailEvent(
        event_id=event_id,
        event_type=internal_type,
        message_id=message_id,
        to_email=to_email,
        occurred_at=occurred_at,
    )


async def load_cursor(
    session_factory: async_sessionmaker, feed: str = POLLER_FEED
) -> Optional[str]:
    async with session_factory() as db:
        row = await db.get(EmailEventsPollerCursor, feed)
        return row.last_cursor if row else None


async def save_cursor(
    session_factory: async_sessionmaker, feed: str, cursor: Optional[str]
) -> None:
    async with session_factory() as db:
        row = await db.get(EmailEventsPollerCursor, feed)
        if row is None:
            db.add(EmailEventsPollerCursor(id=feed, last_cursor=cursor))
        else:
            row.last_cursor = cursor
        await db.commit()


async def fetch_page(
    client: httpx.AsyncClient,
    base_url: str,
    api_key: str,
    cursor: Optional[str],
) -> dict:
    """GET /v2/email/events. Returns the parsed JSON body.

    Raises PollerAPIError on a non-2xx status. Raises httpx.HTTPError on a
    transport-level failure (connect error, timeout). The caller catches
    both and leaves the cursor untouched.
    """
    url = f"{base_url}{EVENTS_PATH}"
    params: dict[str, str] = {"page[size]": str(DEFAULT_PAGE_SIZE)}
    if cursor:
        params["page[cursor]"] = cursor
    headers = {"Authorization": f"Bearer {api_key}"}
    resp = await client.get(url, params=params, headers=headers)
    if resp.status_code != 200:
        raise PollerAPIError(
            f"Email events API returned {resp.status_code}: {resp.text[:200]}",
            resp.status_code,
        )
    return resp.json()


async def poll_once(
    session_factory: async_sessionmaker,
    client: httpx.AsyncClient,
    base_url: str,
    api_key: str,
    feed: str = POLLER_FEED,
) -> PollSummary:
    """One poll cycle. Never raises — the launchd runner can wrap this
    without try/except for control flow.
    """
    summary = PollSummary()
    cursor = await load_cursor(session_factory, feed)

    while True:
        try:
            page = await fetch_page(client, base_url, api_key, cursor)
        except (httpx.HTTPError, PollerAPIError) as e:
            logger.warning(
                "Email events API fetch failed — cursor unchanged",
                error=str(e),
                cursor=cursor,
            )
            summary.errors += 1
            return summary

        items = page.get("data") or []
        meta = page.get("meta") or {}
        page_size = meta.get("page_size", DEFAULT_PAGE_SIZE)
        next_cursor = meta.get("page_cursor")
        summary.pages += 1

        errored = False
        for item in items:
            try:
                event = extract_event_from_api_item(item)
            except (KeyError, TypeError, ValueError) as e:
                logger.warning(
                    "Malformed event item — skipping",
                    error=str(e),
                    item_id=item.get("id"),
                )
                summary.skipped += 1
                continue
            if event is None:
                summary.skipped += 1
                continue
            try:
                async with session_factory() as db:
                    result = await process_email_event(db, event)
            except Exception as e:
                logger.warning(
                    "Event processing failed — cursor will not advance past this page",
                    error=str(e),
                    event_id=event.event_id,
                    event_type=event.event_type,
                )
                summary.errors += 1
                errored = True
                continue
            _tally(summary, result)

        if errored:
            return summary

        cursor = next_cursor
        await save_cursor(session_factory, feed, cursor)
        summary.cursor_advanced = True

        if len(items) < page_size:
            return summary


def _tally(summary: PollSummary, result: dict) -> None:
    if result.get("already_processed"):
        summary.already_processed += 1
    elif result.get("unmatched"):
        summary.unmatched += 1
    elif result.get("processed"):
        summary.processed += 1
    else:
        summary.skipped += 1
