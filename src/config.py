"""Configuration management for Sequence Service."""

from pydantic_settings import BaseSettings
from functools import lru_cache


# =============================================================
# HARDCODED MAILBOX ALLOCATION — DO NOT MODIFY WITHOUT APPROVAL
# =============================================================
# Quinn and Scout have dedicated mailbox pools.
# These cannot be bypassed. Tenant isolation enforces this at
# the API layer; these constants enforce it in code.
# =============================================================

QUINN_MAILBOXES = frozenset({
    "quinn@telnyx.com",
    "quinn.a@telnyx.com",
    "quinn.b@telnyx.com",
})

SCOUT_MAILBOXES = frozenset({
    "quinn.c@telnyx.com",
    "quinn.d@telnyx.com",
    "quinn.e@telnyx.com",
    "quinn.f@telnyx.com",
    "quinn.g@telnyx.com",
    "quinn.h@telnyx.com",
    "quinn.i@telnyx.com",
    "quinn.j@telnyx.com",
})

ALL_ALLOWED_MAILBOXES = QUINN_MAILBOXES | SCOUT_MAILBOXES

TENANT_MAILBOX_MAP = {
    "tenant-quinn": QUINN_MAILBOXES,
    "tenant-scout": SCOUT_MAILBOXES,
}
# =============================================================


class Settings(BaseSettings):
    """Application settings loaded from environment."""
    
    # Database (local Postgres via Homebrew)
    database_url: str = "postgresql+asyncpg://kevinward@localhost:5432/sequence_service"
    
    # Redis
    redis_url: str = "redis://localhost:6379"
    
    # Gmail
    gmail_enabled: bool = False
    gmail_service_account_file: str = "/Users/kevinward/.openclaw/workspace/quinn-v2/credentials/service-account.json"
    gmail_delegated_user: str = "quinn@telnyx.com"
    
    # Tracking
    tracking_enabled: bool = True
    tracking_base_url: str = "http://localhost:8000"  # Override in production

    # CAN-SPAM / unsubscribe compliance (Wave 0). The visible unsubscribe link +
    # physical postal address are ALWAYS added to every email regardless of
    # tracking_enabled (only the open pixel / click-wrap are gated by tracking).
    physical_address: str = "Telnyx LLC, 600 Congress Avenue, 14th Floor, Austin, TX 78701, USA"
    unsubscribe_mailto: str = "mailto:unsubscribe@telnyx.com?subject=unsubscribe"
    # One-click (RFC 8058) unsubscribe requires a PUBLICLY REACHABLE tracking_base_url
    # serving /track/unsubscribe. Until that host exists, keep this False so we do
    # NOT advertise a dead one-click endpoint (track.telnyx.com is NXDOMAIN); the
    # mailto unsubscribe is used instead.
    one_click_unsubscribe_enabled: bool = False
    
    # API
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    
    # Workers
    worker_concurrency: int = 10

    # Stuck-step reconciler (audit M4): re-enqueue SCHEDULED steps whose arq job
    # was lost. A step is reconciled once it is this many seconds past due (or has
    # no scheduled_at). Kept above the max step jitter so we never race a job that
    # is simply waiting on its defer.
    reconcile_grace_seconds: int = 900
    reconcile_batch_limit: int = 200

    # Circuit Breaker
    circuit_breaker_enabled: bool = True
    circuit_breaker_threshold: float = 0.10
    circuit_breaker_window_hours: int = 24
    # Auto-resume: un-pause circuit_breaker-paused enrollments once the mailbox
    # bounce rate cools below this (hysteresis margin under the 0.10 trip line, so
    # a still-elevated mailbox like 8.5% stays paused instead of resuming + re-tripping).
    circuit_breaker_resume_threshold: float = 0.06
    # Max enrollments to resume per mailbox per run, and never more than the
    # mailbox's spare daily capacity — so a recovered backlog trickles in behind
    # in-flight enrollments rather than crowding them out of the shared send cap.
    circuit_breaker_resume_per_run: int = 10

    # Send Window
    send_window_enabled: bool = True
    send_window_start: int = 8   # 8am
    send_window_end: int = 17    # 5pm
    
    # Send Jitter
    send_jitter_enabled: bool = True
    send_jitter_minutes: int = 15
    
    # Logging
    log_level: str = "INFO"
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()


def validate_mailbox_for_tenant(tenant_id: str, email: str) -> bool:
    """
    Validate that a mailbox email is allowed for a given tenant.
    
    Returns True if allowed, raises ValueError if not.
    This is a hardcoded safety check — even if the DB is misconfigured,
    this will block unauthorized mailbox usage.
    """
    allowed = TENANT_MAILBOX_MAP.get(tenant_id)
    if allowed is None:
        # Unknown tenant — allow any mailbox in ALL_ALLOWED_MAILBOXES
        return email in ALL_ALLOWED_MAILBOXES
    if email not in allowed:
        raise ValueError(
            f"Mailbox {email} is not allowed for tenant {tenant_id}. "
            f"Allowed: {sorted(allowed)}"
        )
    return True
