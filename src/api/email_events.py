"""Telnyx Email API webhook receiver (REVOPS-1552).

POST /webhooks/email-events — receives Telnyx Email API delivery events
(delivered/bounce/complaint/one-click-unsubscribe) and dispatches them to
the event processing service after Ed25519 signature verification.

SECURITY CONTRACT:
  The Ed25519 signature is verified over the RAW request body BEFORE any JSON
  parsing. Re-serializing JSON breaks verification — the bytes as received
  are the bytes that were signed. The signed payload is
  ``"{timestamp}|{raw_body}"`` — the timestamp header string, a literal pipe
  ``|``, then the raw body bytes (the official Telnyx webhook contract,
  confirmed via the telnyx-python SDK source). The timestamp is bound into
  the signed message so an attacker cannot swap the timestamp header without
  invalidating the signature. The timestamp is also checked for skew
  (> ``telnyx_webhook_timestamp_tolerance_seconds`` => reject).

  The public key is read from the ``TELNYX_WEBHOOK_PUBLIC_KEY`` env var (name
  only, never a value in the repo). It is a base64-encoded raw 32-byte Ed25519
  public key. Empty/invalid key => reject all events with 401 (fail closed).
  Bad/missing signature or stale timestamp => 401. Nothing unverified is
  ever processed.

  This endpoint is exempt from the X-API-Key tenant-auth middleware (see
  ``src/api/main.py``) — the Ed25519 signature replaces tenant auth for this
  path. The tenant is derived from the matched SentEmail's enrollment chain,
  not from an API key.
"""

from __future__ import annotations

import base64
import json
import time
from typing import Optional

import structlog
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import get_settings
from src.models.base import get_db
from src.services.email_events import (
    EVENT_TYPE_MAP,
    EmailEvent,
    process_email_event,
)

logger = structlog.get_logger()
settings = get_settings()
router = APIRouter()

SIGNATURE_HEADER = "telnyx-signature-ed25519"
TIMESTAMP_HEADER = "telnyx-timestamp"

# Full Telnyx Email API event set (SV2-044: sent/delivered/bounced/opened/clicked/suppressed).
# The raw → internal type map lives in src/services/email_events.py (EVENT_TYPE_MAP)
# and is shared with the events pull-poller (REVOPS-1525) so both the push
# (webhook receiver) and pull (poller) paths map event types identically.


def _load_public_key(key_b64: str) -> Optional[Ed25519PublicKey]:
    if not key_b64:
        return None
    try:
        raw = base64.b64decode(key_b64)
        return Ed25519PublicKey.from_public_bytes(raw)
    except Exception as e:
        logger.error(
            "Invalid TELNYX_WEBHOOK_PUBLIC_KEY — rejecting all events",
            error=str(e),
        )
        return None


def verify_webhook_signature(
    raw_body: bytes,
    signature_header: Optional[str],
    timestamp_header: Optional[str],
    public_key: Ed25519PublicKey,
    tolerance_seconds: int = 300,
) -> bool:
    """Verify the Ed25519 signature and timestamp over the raw request body.

    Signed payload: ``"{timestamp}|{raw_body}"`` (Telnyx contract). The
    timestamp is bound into the signed message so it cannot be swapped
    without invalidating the signature. The timestamp is also checked for
    skew (replay protection).

    Returns ``True`` only if the signature is valid AND the timestamp is
    within ``tolerance_seconds`` of now. Returns ``False`` if the signature
    is bad, the timestamp is stale, or either header is missing.
    """
    if not signature_header or not timestamp_header:
        return False

    try:
        ts = int(timestamp_header)
    except (ValueError, TypeError):
        return False
    if abs(int(time.time()) - ts) > tolerance_seconds:
        return False

    try:
        sig = base64.b64decode(signature_header)
    except Exception:
        return False
    if len(sig) != 64:
        return False

    signed_payload = timestamp_header.encode("utf-8") + b"|" + raw_body
    try:
        public_key.verify(sig, signed_payload)
        return True
    except InvalidSignature:
        return False


def _extract_event(payload: dict) -> EmailEvent:
    """Extract a normalized EmailEvent from the Telnyx webhook envelope.

    Raises KeyError if a required field is missing. The caller catches and
    returns 400 (signed but malformed — no crash, no partial writes).
    """
    data = payload["data"]
    raw_type = data["event_type"]
    event_type = EVENT_TYPE_MAP.get(raw_type)
    if event_type is None:
        raise ValueError(f"Unknown event type: {raw_type}")

    event_id = data["id"]
    occurred_at = data.get("occurred_at")

    p = data["payload"]
    message_id = p["id"]
    to_raw = p.get("to")
    if isinstance(to_raw, list):
        to_email = to_raw[0] if to_raw else ""
    else:
        to_email = to_raw or ""

    return EmailEvent(
        event_id=event_id,
        event_type=event_type,
        message_id=message_id,
        to_email=to_email,
        occurred_at=occurred_at,
    )


@router.post("/email-events")
async def receive_email_event(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Receive and process a Telnyx Email API webhook event.

    Returns 200 on successful processing or idempotent no-op. Returns 401
    on bad/missing signature or stale timestamp (fail closed — nothing
    unverified is processed). Returns 400 on a signed-but-malformed payload
    (no crash, no partial writes).
    """
    raw_body = await request.body()

    public_key = _load_public_key(settings.telnyx_webhook_public_key)
    if public_key is None:
        logger.error(
            "Webhook public key not configured — rejecting event (fail closed)"
        )
        return JSONResponse(
            status_code=401,
            content={"detail": "Webhook verification not configured"},
        )

    sig = request.headers.get(SIGNATURE_HEADER)
    ts = request.headers.get(TIMESTAMP_HEADER)

    if not verify_webhook_signature(
        raw_body,
        sig,
        ts,
        public_key,
        settings.telnyx_webhook_timestamp_tolerance_seconds,
    ):
        logger.warning(
            "Webhook signature/timestamp verification failed",
            has_signature=bool(sig),
            has_timestamp=bool(ts),
        )
        return JSONResponse(
            status_code=401,
            content={"detail": "Invalid signature or timestamp"},
        )

    try:
        payload = json.loads(raw_body)
    except json.JSONDecodeError:
        logger.warning("Webhook body is not valid JSON (signature valid)")
        return JSONResponse(status_code=400, content={"detail": "Malformed JSON"})

    try:
        event = _extract_event(payload)
    except (KeyError, TypeError, ValueError) as e:
        logger.warning(
            "Webhook payload missing required fields",
            error=str(e),
        )
        return JSONResponse(
            status_code=400, content={"detail": "Missing required fields"}
        )

    try:
        result = await process_email_event(db, event)
    except Exception as e:
        logger.error(
            "Webhook event processing failed",
            error=str(e),
            event_id=getattr(event, "event_id", None),
        )
        return JSONResponse(status_code=500, content={"detail": "Processing failed"})

    return JSONResponse(status_code=200, content={"data": result})
