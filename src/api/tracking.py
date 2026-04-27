"""Tracking endpoints for open/click detection and unsubscribe (RFC 8058)."""

import base64
import uuid
from datetime import datetime
from urllib.parse import unquote
import structlog
from fastapi import APIRouter, Response, HTTPException
from fastapi.responses import RedirectResponse, HTMLResponse
from sqlalchemy import select, update

from src.models.base import async_session
from src.models.models import (
    SentEmail, Signal, SignalType, SequenceEnrollmentStep, SequenceEnrollment,
    Suppression, SuppressionReason,
)
from src.services.webhooks import create_signal_webhook

logger = structlog.get_logger()
router = APIRouter()

# 1x1 transparent GIF (43 bytes)
TRACKING_PIXEL = base64.b64decode(
    "R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7"
)


def encode_tracking_id(sent_email_id: str, link_url: str = "") -> str:
    """Encode sent_email_id and optional URL for tracking."""
    data = f"{sent_email_id}|{link_url}"
    return base64.urlsafe_b64encode(data.encode()).decode()


def decode_tracking_id(tracking_id: str) -> tuple[str, str]:
    """Decode tracking_id back to sent_email_id and optional URL."""
    try:
        data = base64.urlsafe_b64decode(tracking_id.encode()).decode()
        parts = data.split("|", 1)
        sent_email_id = parts[0]
        link_url = parts[1] if len(parts) > 1 else ""
        return sent_email_id, link_url
    except Exception:
        return "", ""


@router.get("/open/{tracking_id}")
async def track_open(tracking_id: str):
    """
    Track email open via invisible pixel.
    
    Returns a 1x1 transparent GIF and logs the open signal.
    """
    sent_email_id, _ = decode_tracking_id(tracking_id)
    
    if not sent_email_id:
        # Return pixel anyway to not break email rendering
        return Response(content=TRACKING_PIXEL, media_type="image/gif")
    
    # Log open asynchronously (don't block pixel response)
    try:
        async with async_session() as db:
            # Check sent email exists
            result = await db.execute(
                select(SentEmail).where(SentEmail.id == sent_email_id)
            )
            sent_email = result.scalar_one_or_none()
            
            if sent_email:
                # Check if we already logged an open for this email
                existing = await db.execute(
                    select(Signal).where(
                        Signal.sent_email_id == sent_email_id,
                        Signal.type == SignalType.OPEN,
                    )
                )
                
                if not existing.scalar_one_or_none():
                    # First open - create signal
                    signal = Signal(
                        id=str(__import__('uuid').uuid4()),
                        sent_email_id=sent_email_id,
                        type=SignalType.OPEN,
                        detected_at=datetime.utcnow(),
                        raw_data='{"source": "tracking_pixel"}',
                    )
                    db.add(signal)
                    await db.flush()
                    
                    logger.info(
                        "Open tracked",
                        sent_email_id=sent_email_id,
                        to_email=sent_email.to_email,
                    )
                    
                    # Trigger webhook
                    try:
                        from sqlalchemy.orm import selectinload
                        from sqlalchemy import select as sa_select
                        result = await db.execute(
                            sa_select(SequenceEnrollmentStep)
                            .where(SequenceEnrollmentStep.id == sent_email.enrollment_step_id)
                            .options(selectinload(SequenceEnrollmentStep.enrollment))
                        )
                        enrollment_step = result.scalar_one_or_none()
                        if enrollment_step:
                            await create_signal_webhook(db, signal, sent_email, enrollment_step.enrollment)
                    except Exception as e:
                        logger.error("Failed to create open webhook", error=str(e))
                    
                    await db.commit()
    except Exception as e:
        logger.error("Failed to log open", error=str(e))
    
    # Always return the pixel
    return Response(
        content=TRACKING_PIXEL,
        media_type="image/gif",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        }
    )


@router.get("/click/{tracking_id}")
async def track_click(tracking_id: str):
    """
    Track link click and redirect to original URL.
    
    Logs the click signal and redirects user to the actual link.
    """
    sent_email_id, link_url = decode_tracking_id(tracking_id)
    
    if not sent_email_id or not link_url:
        raise HTTPException(status_code=400, detail="Invalid tracking link")
    
    # Decode URL (might be URL-encoded)
    link_url = unquote(link_url)
    
    # Log click
    try:
        async with async_session() as db:
            result = await db.execute(
                select(SentEmail).where(SentEmail.id == sent_email_id)
            )
            sent_email = result.scalar_one_or_none()
            
            if sent_email:
                # Log every click (don't dedupe like opens)
                signal = Signal(
                    id=str(__import__('uuid').uuid4()),
                    sent_email_id=sent_email_id,
                    type=SignalType.CLICK,
                    detected_at=datetime.utcnow(),
                    raw_data=f'{{"url": "{link_url}"}}',
                )
                db.add(signal)
                await db.flush()
                
                logger.info(
                    "Click tracked",
                    sent_email_id=sent_email_id,
                    url=link_url,
                    to_email=sent_email.to_email,
                )
                
                # Trigger webhook
                try:
                    from sqlalchemy.orm import selectinload
                    from sqlalchemy import select as sa_select
                    result = await db.execute(
                        sa_select(SequenceEnrollmentStep)
                        .where(SequenceEnrollmentStep.id == sent_email.enrollment_step_id)
                        .options(selectinload(SequenceEnrollmentStep.enrollment))
                    )
                    enrollment_step = result.scalar_one_or_none()
                    if enrollment_step:
                        await create_signal_webhook(db, signal, sent_email, enrollment_step.enrollment)
                except Exception as e:
                    logger.error("Failed to create click webhook", error=str(e))
                
                await db.commit()
    except Exception as e:
        logger.error("Failed to log click", error=str(e))
    
    # Redirect to original URL
    return RedirectResponse(url=link_url, status_code=302)


def generate_tracking_pixel_url(base_url: str, sent_email_id: str) -> str:
    """Generate the tracking pixel URL for an email."""
    tracking_id = encode_tracking_id(sent_email_id)
    return f"{base_url}/track/open/{tracking_id}"


def wrap_link_for_tracking(base_url: str, sent_email_id: str, original_url: str) -> str:
    """Wrap a link URL for click tracking."""
    tracking_id = encode_tracking_id(sent_email_id, original_url)
    return f"{base_url}/track/click/{tracking_id}"


# ---------------------------------------------------------------------------
# Unsubscribe tracking helpers (RFC 8058)
# ---------------------------------------------------------------------------

def generate_unsubscribe_id(enrollment_id: str) -> str:
    """Encode an enrollment_id for use in unsubscribe URLs."""
    data = f"unsub|{enrollment_id}"
    return base64.urlsafe_b64encode(data.encode()).decode()


def decode_unsubscribe_id(token: str) -> str:
    """Decode an unsubscribe token back to enrollment_id. Returns '' on failure."""
    try:
        data = base64.urlsafe_b64decode(token.encode()).decode()
        if not data.startswith("unsub|"):
            return ""
        return data.split("|", 1)[1]
    except Exception:
        return ""


def generate_unsubscribe_url(base_url: str, enrollment_id: str) -> str:
    """Generate the full unsubscribe URL for an enrollment."""
    token = generate_unsubscribe_id(enrollment_id)
    return f"{base_url}/track/unsubscribe/{token}"


@router.post("/unsubscribe/{tracking_id}")
@router.get("/unsubscribe/{tracking_id}")  # GET for email client one-click
async def handle_unsubscribe(tracking_id: str):
    """
    Handle unsubscribe request (RFC 8058 one-click + link click).

    Always returns 200 regardless of whether the enrollment exists so as not to
    leak information about valid IDs (per RFC 8058 guidance).
    """
    enrollment_id = decode_unsubscribe_id(tracking_id)

    if not enrollment_id:
        # Invalid token — return 200 with confirmation page anyway (don't leak info)
        return HTMLResponse(
            "<html><body><p>You have been unsubscribed.</p></body></html>"
        )

    try:
        async with async_session() as db:
            # Load enrollment with its sequence (need tenant_id from sequence)
            from sqlalchemy.orm import selectinload

            result = await db.execute(
                select(SequenceEnrollment)
                .where(SequenceEnrollment.id == enrollment_id)
                .options(selectinload(SequenceEnrollment.sequence))
            )
            enrollment = result.scalar_one_or_none()

            if not enrollment:
                return HTMLResponse(
                    "<html><body><p>You have been unsubscribed.</p></body></html>"
                )

            tenant_id = enrollment.sequence.tenant_id

            # Add to suppression list (ignore duplicate — unique constraint)
            try:
                suppression = Suppression(
                    id=str(uuid.uuid4()),
                    tenant_id=tenant_id,
                    email=enrollment.contact_email,
                    domain=enrollment.contact_email.split("@", 1)[1].lower()
                    if "@" in enrollment.contact_email
                    else None,
                    reason=SuppressionReason.UNSUBSCRIBE,
                    source_enrollment_id=enrollment_id,
                )
                db.add(suppression)
                await db.flush()
            except Exception:
                # Likely duplicate suppression — that's fine
                await db.rollback()

            # Cancel ALL active enrollments for this contact across the tenant
            await db.execute(
                update(SequenceEnrollment)
                .where(SequenceEnrollment.contact_email == enrollment.contact_email)
                .where(SequenceEnrollment.status == "ACTIVE")
                .values(status="UNSUBSCRIBED")
            )

            await db.commit()

            logger.info(
                "Unsubscribe processed",
                enrollment_id=enrollment_id,
                contact_email=enrollment.contact_email,
                tenant_id=tenant_id,
            )
    except Exception as e:
        logger.error("Failed to process unsubscribe", error=str(e))

    return HTMLResponse(
        "<html><body><p>You have been unsubscribed.</p></body></html>"
    )
