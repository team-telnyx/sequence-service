"""Tests for the Scout-only collapse of src/config.py (REVOPS-972 / M4 / QC-4).

After the collapse:
  - validate_mailbox_for_tenant is a single SCOUT_MAILBOXES membership check.
  - There is NO unknown-tenant fallback that can reach a mailbox.
  - QUINN_MAILBOXES no longer exists.
  - The Gmail service-account path no longer lives under the quinn-v2 dir,
    and gmail_delegated_user is gone.
  - Settings still imports cleanly and never uses extra='forbid'.
"""

import importlib

import pytest

import src.config as config


SCOUT_OK_COM = [
    "quinn.c@telnyx.com",
    "quinn.d@telnyx.com",
    "quinn.e@telnyx.com",
    "quinn.f@telnyx.com",
    "quinn.g@telnyx.com",
    "quinn.h@telnyx.com",
    "quinn.i@telnyx.com",
    "quinn.j@telnyx.com",
]

# Email API warm-up lane (REVOPS-1525; approval Kevin 2026-09-08).
SCOUT_OK_CO = [
    "quinn.c@telnyx.co",
    "quinn.d@telnyx.co",
    "quinn.e@telnyx.co",
    "quinn.f@telnyx.co",
    "quinn.g@telnyx.co",
    "quinn.h@telnyx.co",
    "quinn.i@telnyx.co",
    "quinn.j@telnyx.co",
]

SCOUT_OK = SCOUT_OK_COM + SCOUT_OK_CO


@pytest.mark.parametrize("email", SCOUT_OK)
def test_validate_accepts_all_scout_mailboxes(email):
    assert config.validate_mailbox_for_tenant("tenant-scout", email) is True


def test_scout_mailboxes_membership_is_exactly_c_through_j_both_lanes():
    assert config.SCOUT_MAILBOXES == frozenset(SCOUT_OK)


def test_validate_rejects_unknown_mailbox_for_scout():
    with pytest.raises(ValueError):
        config.validate_mailbox_for_tenant("tenant-scout", "stranger@telnyx.com")


def test_validate_rejects_foreign_domain_mailbox():
    # .com is allowed, .co is allowed, but any other domain is not — even
    # the same local-part on a foreign domain must be rejected.
    for email in (
        "quinn.c@example.com",
        "quinn.c@telnyx.org",
        "quinn.c@gmail.com",
        "stranger@telnyx.co",
        "quinn.k@telnyx.co",
        "quinn.b@telnyx.co",
    ):
        with pytest.raises(ValueError):
            config.validate_mailbox_for_tenant("tenant-scout", email)


def test_validate_accepts_co_lane_for_tenant_scout():
    # The .co warm-up lane is allowlisted for tenant-scout (REVOPS-1525).
    for email in SCOUT_OK_CO:
        assert config.validate_mailbox_for_tenant("tenant-scout", email) is True


def test_validate_rejects_former_quinn_pool_mailbox():
    # quinn@/quinn.a@/quinn.b@ were the Quinn pool — they must NOT validate now.
    # The .co lane does NOT revive them either.
    for email in (
        "quinn@telnyx.com",
        "quinn.a@telnyx.com",
        "quinn.b@telnyx.com",
        "quinn@telnyx.co",
        "quinn.a@telnyx.co",
        "quinn.b@telnyx.co",
    ):
        with pytest.raises(ValueError):
            config.validate_mailbox_for_tenant("tenant-scout", email)


def test_no_unknown_tenant_fallback_to_non_scout_mailbox():
    """A typo/unknown tenant must NOT be able to reach ANY mailbox.

    Pre-collapse, an unknown tenant fell back to ALL_ALLOWED_MAILBOXES (which
    included the Quinn pool) and could validate a non-Scout mailbox. Post-collapse
    the check is a pure SCOUT_MAILBOXES membership test for ``tenant-scout`` only
    — with NO escape hatch: any tenant other than ``tenant-scout`` is rejected
    regardless of the mailbox, and any mailbox outside the Scout pool is rejected
    for ``tenant-scout``. The .co warm-up lane does not open a new escape hatch —
    foreign domains and the retired Quinn pool are rejected for every tenant
    string, and a Scout mailbox is rejected for every non-scout tenant string.
    """
    for tenant in ("tenant-typo", "tenant-quinn", ""):
        # Non-Scout mailbox → rejected for any tenant (mailbox check).
        with pytest.raises(ValueError):
            config.validate_mailbox_for_tenant(tenant, "quinn.a@telnyx.com")
        with pytest.raises(ValueError):
            config.validate_mailbox_for_tenant(tenant, "stranger@telnyx.com")
        with pytest.raises(ValueError):
            config.validate_mailbox_for_tenant(tenant, "stranger@telnyx.co")
        with pytest.raises(ValueError):
            config.validate_mailbox_for_tenant(tenant, "quinn.a@telnyx.co")
        # Scout mailbox → STILL rejected for any non-scout tenant (tenant gate).
        with pytest.raises(ValueError):
            config.validate_mailbox_for_tenant(tenant, "quinn.c@telnyx.com")
        with pytest.raises(ValueError):
            config.validate_mailbox_for_tenant(tenant, "quinn.c@telnyx.co")


def test_scout_mailbox_rejected_for_non_scout_tenant():
    """The tenant gate is authoritative: a valid Scout sender is REJECTED when
    the tenant is anything other than ``tenant-scout``. The r1 loosening that
    accepted any tenant string as long as the mailbox was in SCOUT_MAILBOXES
    was a regression of the documented no-escape-hatch — restored here. Both
    lanes (.com and .co) behave the same way: a .co warm-up mailbox is rejected
    for any non-scout tenant string."""
    for tenant in ("anything", "tenant-quinn", "tenant-typo", ""):
        with pytest.raises(ValueError):
            config.validate_mailbox_for_tenant(tenant, "quinn.c@telnyx.com")
        with pytest.raises(ValueError):
            config.validate_mailbox_for_tenant(tenant, "quinn.c@telnyx.co")
    # The one tenant that IS allowed passes for both lanes.
    assert (
        config.validate_mailbox_for_tenant("tenant-scout", "quinn.c@telnyx.com") is True
    )
    assert (
        config.validate_mailbox_for_tenant("tenant-scout", "quinn.c@telnyx.co") is True
    )


def test_quinn_mailboxes_symbol_removed():
    assert not hasattr(config, "QUINN_MAILBOXES")


def test_gmail_delegated_user_removed():
    settings = config.Settings()
    assert not hasattr(settings, "gmail_delegated_user")


def test_gmail_service_account_path_not_under_quinn_v2():
    settings = config.Settings()
    assert "quinn-v2" not in settings.gmail_service_account_file


def test_settings_never_forbids_extra_env():
    """extra must stay 'ignore' — never 'forbid' (L1: plists set dead env)."""
    settings = config.Settings()
    extra = settings.model_config.get("extra")
    assert extra != "forbid"


def test_config_imports_cleanly():
    importlib.reload(config)
    assert config.get_settings() is not None
