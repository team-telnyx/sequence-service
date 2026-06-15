"""Configuration management for Sequence Service."""

from pydantic_settings import BaseSettings, SettingsConfigDict
from functools import lru_cache


# =============================================================
# HARDCODED MAILBOX ALLOCATION — DO NOT MODIFY WITHOUT APPROVAL
# =============================================================
# Scout-only deployment (REVOPS-972 / M4 / QC-4). The service runs a single
# tenant (tenant-scout) and sends ONLY through the 8 Scout sender inboxes
# quinn.c–quinn.j. The Quinn pool, the multi-tenant TENANT_MAILBOX_MAP, and the
# unknown-tenant ALL_ALLOWED_MAILBOXES fallback are removed: a single
# SCOUT_MAILBOXES membership check (validate_mailbox_for_tenant, below) is the
# in-code safety net even if the DB is misconfigured, with NO escape hatch.
# (quinn.c–j are physical inboxes owned by Scout; the "quinn." local-part is
# legacy naming, not the retired tenant-quinn pool.)
# =============================================================

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

# Transitional Scout-only shims. The Quinn pool and the unknown-tenant fallback
# semantics are GONE — these intentionally resolve to ONLY the Scout pool. They
# exist solely so services.mailbox_rotation keeps importing while its own
# Scout-only collapse lands in the sibling mailbox-rotation workstream; both
# names are DELETED once that merges (the membership check above is canonical).
ALL_ALLOWED_MAILBOXES = SCOUT_MAILBOXES
TENANT_MAILBOX_MAP = {"tenant-scout": SCOUT_MAILBOXES}
# =============================================================


class Settings(BaseSettings):
    """Application settings loaded from environment."""

    # extra='ignore' is REQUIRED (audit L1): plists set env the service does not
    # read (SCOUT_API_KEY, SEQUENCE_SEND_MODE, GMAIL_MAILBOXES, ...). pydantic
    # defaults to 'forbid', which would reject those and crash startup. NEVER
    # set this to 'forbid'.
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Database (local Postgres via Homebrew)
    database_url: str = "postgresql+asyncpg://kevinward@localhost:5432/sequence_service"

    # Redis
    redis_url: str = "redis://localhost:6379"

    # Gmail
    gmail_enabled: bool = False
    # Scout-owned service-account path (REVOPS-972 / M5). Relocated off the
    # retired quinn-v2 directory. NOTE: this updates the config DEFAULT only —
    # the actual credentials file is physically moved to this path at cutover
    # (maintenance window), not by this change. Domain-wide delegation must be
    # confirmed for quinn.c–quinn.j before flip. Sends delegate per-inbox via
    # gmail's with_subject(self.inbox); there is no single delegated user.
    gmail_service_account_file: str = "/Users/kevinward/.openclaw-scout/credentials/service-account.json"

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


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()


def validate_mailbox_for_tenant(tenant_id: str, email: str) -> bool:
    """
    Validate that a mailbox email is allowed to send.

    Scout-only (REVOPS-972 / M4): the single allowed pool is SCOUT_MAILBOXES.
    Returns True if allowed, raises ValueError otherwise. This is the hardcoded
    safety check — even if the DB is misconfigured, it blocks any non-Scout
    mailbox, and there is NO unknown-tenant fallback that could reach a mailbox.

    `tenant_id` is retained in the signature for call-site compatibility
    (enrollments.py, sequence_step.py) but the check is the same single Scout
    allowlist regardless of tenant.
    """
    if email not in SCOUT_MAILBOXES:
        raise ValueError(
            f"Mailbox {email} is not an allowed Scout sender "
            f"(tenant {tenant_id}). Allowed: {sorted(SCOUT_MAILBOXES)}"
        )
    return True
