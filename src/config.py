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

SCOUT_MAILBOXES = frozenset(
    {
        "quinn.c@telnyx.com",
        "quinn.d@telnyx.com",
        "quinn.e@telnyx.com",
        "quinn.f@telnyx.com",
        "quinn.g@telnyx.com",
        "quinn.h@telnyx.com",
        "quinn.i@telnyx.com",
        "quinn.j@telnyx.com",
    }
)

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
    # read (SCOUT_API_URL, SEQUENCE_SEND_MODE, GMAIL_MAILBOXES, ...). pydantic
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
    gmail_service_account_file: str = (
        "/Users/kevinward/.openclaw-scout/credentials/service-account.json"
    )

    # CAN-SPAM compliance. No first-party /track/unsubscribe endpoint —
    # one-click is handled by the Telnyx Email API webhook (email.unsubscribed).
    physical_address: str = "Telnyx LLC, 600 Congress Avenue, 14th Floor, Austin, TX 78701, USA"
    unsubscribe_mailto: str = "mailto:unsubscribe@telnyx.com?subject=unsubscribe"

    # Email-to-Salesforce task logging (Kevin 2026-07-10): every outbound send is
    # BCC'd here so SFDC auto-logs a completed Task on the matching contact/lead,
    # replacing the manual post-send task entry. Blank disables the BCC. NOTE:
    # SFDC only accepts these if the SENDING address (quinn.c–j@telnyx.com) is in
    # the owning user's "My Acceptable Email Addresses" in Email-to-Salesforce
    # setup — otherwise messages are silently discarded.
    salesforce_bcc_address: str = (
        "emailtosalesforce@a-33gccorss1hb49mgd2oqu29yotdtrci512h16g9oae45z3evgp"
        ".j-1nifjeaq.usa416.le.salesforce.com"
    )

    # API
    # Loopback by default; non-loopback binding requires explicit exposure + ADR.
    api_host: str = "127.0.0.1"
    api_port: int = 8000

    # Auth — the X-API-Key value the middleware validates against. NAME only
    # is read from env; the VALUE never appears in the repo. Fail-closed: if
    # unset/empty, protected paths return 503 (service not configured for auth).
    sequence_service_api_key: str = ""

    # CORS — explicit origins only. Wildcard+credentials is a CSRF surface
    # (forbidden). Empty list = no CORS (fail-closed). Comma-separated env var.
    cors_allowed_origins: list[str] = []

    # Workers
    worker_concurrency: int = 10
    # REVOPS-1552 r4: single source of truth for the arq retry ceiling.
    # Both WorkerSettings.max_tries and the last-attempt terminal check in
    # process_sequence_step read this. If they drift, the worker gives up
    # after N tries while the handler keeps raising ArqRetry on the Nth —
    # the row stays SCHEDULED and the reconciler resurrects it (the r4
    # bug). On the final permitted attempt (job_try >= this) a retryable
    # error is converted to a durable terminal FAILED, not a Retry.
    worker_max_tries: int = 3

    # Stuck-step reconciler (audit M4): re-enqueue SCHEDULED steps whose arq job
    # was lost. A step is reconciled once it is this many seconds past due (or has
    # no scheduled_at). Kept above the max step jitter so we never race a job that
    # is simply waiting on its defer.
    reconcile_grace_seconds: int = 900
    # REVOPS-1552 r3: cap the arq Retry defer for a retryable Email API error at
    # this fraction of the reconciler grace. Without it, a long Retry-After
    # (e.g. 1200s) would defer the arq job past the grace window (900s), so the
    # reconciler would see the step as "lost" and re-enqueue a second job —
    # double-enqueue. The cap is derived from the SAME config the reconciler
    # reads (reconcile_grace_seconds) so the two stay coupled. See
    # _compute_retry_defer in sequence_step.py.
    retry_defer_grace_fraction: float = 0.5
    reconcile_batch_limit: int = 200
    # Per-mailbox capacity pacing (REVOPS-1378 / 2026-07-20 incident): the
    # reconciler previously re-enqueued up to reconcile_batch_limit(200) past-due
    # steps with delay_seconds=None every 10 min — unpaced. Live 2026-07-20 this
    # burned AMER's entire 300/day budget in one hour (251 sends in the 13:00 UTC
    # hour; all 8 mailboxes at sent_today=daily_send_limit=75 by 09:30 ET),
    # crowding out organic follow-ups and net-new admissions. These two settings
    # mirror the existing circuit_resume.py:124-138 precedent: cap each
    # mailbox's reconciliation per sweep, and reserve a fraction of
    # daily_send_limit that the reconciler may never touch, so catch-up trickles
    # in behind in-flight work instead of consuming the shared send cap.
    reconcile_per_mailbox_per_run: int = 10
    reconcile_new_send_reserve_fraction: float = 0.30
    # The daily usable allowance is spread across this many hours so the
    # reconciler trickles catch-up across the whole send window instead of
    # emitting the full per-mailbox daily allowance in the first hour. Matches
    # send_window_start=8 -> send_window_end=17 (REVOPS-1378 / 2026-07-20 review
    # blocker 2: floor alone bounds the daily total, not the hourly rate).
    reconcile_pacing_window_hours: int = 9
    # MUST match the cron cadence in src/workers/main.py (minute=set(range(0,60,10))
    # -> every 10 min). The two change together; if the cron is retuned this must
    # track it or the allowance math mis-spreads the daily budget.
    reconcile_sweep_minutes: int = 10

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
    send_window_start: int = 8  # 8am
    send_window_end: int = 17  # 5pm

    # Send Jitter
    send_jitter_enabled: bool = True
    send_jitter_minutes: int = 15

    # Min spacing between consecutive catch-up touches (REVOPS-1376). When a
    # step's absolute target is already in the past, schedule it min_gap from
    # now instead of firing immediately (prevents back-to-back catch-up sends).
    min_step_gap_hours: int = 24

    # Telnyx Email API webhook receiver (REVOPS-1552). The Ed25519 public key
    # used to verify inbound webhook event signatures (delivered/bounce/
    # complaint/one-click-unsubscribe). The key is provisioned in the Telnyx
    # portal; only the NAME is read from env — the key VALUE never appears in
    # the repo. Empty string disables verification (the endpoint will reject
    # all events with 401 until a key is set — fail closed, never accept an
    # unverified event).
    telnyx_webhook_public_key: str = ""
    # Reject webhook events whose timestamp header is more than this many
    # seconds from now (replay protection). Default 300s = 5 minutes.
    telnyx_webhook_timestamp_tolerance_seconds: int = 300

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
