"""Send window: restrict email sends to business hours in recipient's timezone."""

from datetime import datetime, timedelta

import structlog
from zoneinfo import ZoneInfo

from src.config import get_settings

logger = structlog.get_logger()
settings = get_settings()


def check_send_window(recipient_timezone: str = "America/New_York") -> int | None:
    """
    Check if the current time is within the send window for the recipient's timezone.

    Returns None if within window (OK to send).
    Returns delay_seconds to the next window opening if outside.
    """
    if not settings.send_window_enabled:
        return None

    try:
        tz = ZoneInfo(recipient_timezone)
    except (KeyError, Exception):
        logger.warning("Invalid timezone, defaulting", timezone=recipient_timezone)
        tz = ZoneInfo("America/New_York")

    now_local = datetime.now(tz)
    current_hour = now_local.hour
    current_weekday = now_local.isoweekday()  # 1=Mon, 7=Sun

    # Block weekends (Saturday=6, Sunday=7)
    if current_weekday >= 6:
        # Calculate seconds until Monday 8AM
        days_until_monday = 8 - current_weekday  # Sat→2, Sun→1
        next_open = now_local.replace(
            hour=settings.send_window_start, minute=0, second=0, microsecond=0
        ) + timedelta(days=days_until_monday)
        delay_seconds = int((next_open - now_local).total_seconds())
        logger.info(
            "Weekend — deferring to Monday",
            recipient_timezone=recipient_timezone,
            current_day=now_local.strftime("%A"),
            delay_seconds=delay_seconds,
        )
        return delay_seconds

    if settings.send_window_start <= current_hour < settings.send_window_end:
        return None  # Within window

    # Calculate seconds until next window opening
    if current_hour >= settings.send_window_end:
        # After window today — next opening is tomorrow
        next_open = now_local.replace(
            hour=settings.send_window_start, minute=0, second=0, microsecond=0
        ) + timedelta(days=1)
    else:
        # Before window today
        next_open = now_local.replace(
            hour=settings.send_window_start, minute=0, second=0, microsecond=0
        )

    # If next_open lands on a weekend, push to Monday
    while next_open.isoweekday() >= 6:
        next_open += timedelta(days=1)

    delay_seconds = int((next_open - now_local).total_seconds())
    logger.info(
        "Outside send window, deferring",
        recipient_timezone=recipient_timezone,
        current_hour=current_hour,
        delay_seconds=delay_seconds,
    )
    return delay_seconds
