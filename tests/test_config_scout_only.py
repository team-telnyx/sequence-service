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


SCOUT_OK = [
    "quinn.c@telnyx.com",
    "quinn.d@telnyx.com",
    "quinn.e@telnyx.com",
    "quinn.f@telnyx.com",
    "quinn.g@telnyx.com",
    "quinn.h@telnyx.com",
    "quinn.i@telnyx.com",
    "quinn.j@telnyx.com",
]


@pytest.mark.parametrize("email", SCOUT_OK)
def test_validate_accepts_all_scout_mailboxes(email):
    assert config.validate_mailbox_for_tenant("tenant-scout", email) is True


def test_scout_mailboxes_membership_is_exactly_c_through_j():
    assert config.SCOUT_MAILBOXES == frozenset(SCOUT_OK)


def test_validate_rejects_unknown_mailbox_for_scout():
    with pytest.raises(ValueError):
        config.validate_mailbox_for_tenant("tenant-scout", "stranger@telnyx.com")


def test_validate_rejects_former_quinn_pool_mailbox():
    # quinn@/quinn.a@/quinn.b@ were the Quinn pool — they must NOT validate now.
    for email in ("quinn@telnyx.com", "quinn.a@telnyx.com", "quinn.b@telnyx.com"):
        with pytest.raises(ValueError):
            config.validate_mailbox_for_tenant("tenant-scout", email)


def test_no_unknown_tenant_fallback_to_non_scout_mailbox():
    """A typo/unknown tenant must NOT be able to reach a NON-Scout mailbox.

    Pre-collapse, an unknown tenant fell back to ALL_ALLOWED_MAILBOXES (which
    included the Quinn pool) and could validate a non-Scout mailbox. Post-collapse
    the check is a pure SCOUT_MAILBOXES membership test with no escape hatch:
    any mailbox outside the Scout pool is rejected for ANY tenant string.
    """
    for tenant in ("tenant-typo", "tenant-quinn", ""):
        with pytest.raises(ValueError):
            config.validate_mailbox_for_tenant(tenant, "quinn.a@telnyx.com")
        with pytest.raises(ValueError):
            config.validate_mailbox_for_tenant(tenant, "stranger@telnyx.com")


def test_scout_mailbox_allowed_regardless_of_tenant_string():
    """The collapsed check is email-only: a valid Scout sender is allowed even
    if the tenant_id passed in is unexpected (tenant is retained only for the
    call-site signature / error message)."""
    assert config.validate_mailbox_for_tenant("anything", "quinn.c@telnyx.com") is True


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
