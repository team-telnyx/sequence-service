"""Guard test for REVOPS-1376 backlog remediation DB-identity check.

The migration script (scripts/remediate_backlog_REVOPS-1376.py) must abort
if connected to any DB other than 'sequence_service'. The repo .env points
at the scout DB, so a default run would silently target the wrong schema.
The script filename has a dash so it cannot be imported as a normal module;
it is loaded via importlib.util.spec_from_file_location.
"""

import importlib.util
import os
from unittest.mock import MagicMock, patch

import pytest

SCRIPT_PATH = os.path.join(
    os.path.dirname(__file__), "..", "scripts", "remediate_backlog_REVOPS-1376.py"
)


@pytest.fixture
def migration_mod():
    """Load the dash-named script via importlib (can't import normally)."""
    spec = importlib.util.spec_from_file_location("remediate_backlog", SCRIPT_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _fake_conn_for(dbname):
    """Build a fake psycopg2 conn whose cursor returns the given db name."""
    fake_cursor = MagicMock()
    fake_cursor.fetchone.return_value = (dbname,)
    fake_cursor.__enter__.return_value = fake_cursor

    fake_conn = MagicMock()
    fake_conn.cursor.return_value = fake_cursor
    return fake_conn


def test_aborts_when_connected_to_wrong_db(migration_mod, monkeypatch):
    """Guard must sys.exit('ABORT...') when current_database() != sequence_service."""
    monkeypatch.setattr("sys.argv", ["remediate_backlog"])  # dry-run (no --apply)

    fake_conn = _fake_conn_for("scout")

    with patch("psycopg2.connect", return_value=fake_conn):
        with pytest.raises(SystemExit) as exc_info:
            migration_mod.main()

    msg = str(exc_info.value)
    assert "ABORT" in msg
    assert "scout" in msg
    assert "sequence_service" in msg


def test_passes_when_connected_to_sequence_service(migration_mod, monkeypatch):
    """Guard does not exit when current_database() == sequence_service; dry_run runs."""
    monkeypatch.setattr("sys.argv", ["remediate_backlog"])  # dry-run (no --apply)

    fake_conn = _fake_conn_for("sequence_service")

    with (
        patch.object(migration_mod, "dry_run") as mock_dry_run,
        patch("psycopg2.connect", return_value=fake_conn),
    ):
        migration_mod.main()

    mock_dry_run.assert_called_once()
