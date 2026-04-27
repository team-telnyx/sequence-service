# Sequence Service

Email sequencing engine for Scout V3. Manages enrollment-based email campaigns with send windows, circuit breakers, mailbox rotation, and reply detection.

## Architecture

- **API** (`src/api/`) — FastAPI REST API on port 8001 (enrollments, sequences, mailboxes, tracking, suppressions)
- **Workers** (`src/workers/`) — Arq-based async workers (sequence step execution, signal detection, webhook delivery)
- **Services** (`src/services/`) — Business logic (Gmail integration, circuit breaker, email builder, mailbox rotation, send windows, suppression, templates)
- **Models** (`src/models/`) — SQLAlchemy async models (PostgreSQL)

## Entry Points

| Script | Purpose |
|--------|---------|
| `src/api/main.py` | FastAPI app (uvicorn) |
| `run_arq_worker.py` | Arq task worker |
| `simple_worker.py` | Lightweight polling worker |
| `poll_replies.py` | Gmail reply detection poller |
| `start-worker.sh` | Worker startup helper |

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# Edit .env with your local config
```

## Dependencies

- PostgreSQL (local, asyncpg)
- Redis (local, for arq job queue)
- Google Workspace service account (Gmail API)

## LaunchAgents

macOS LaunchAgent templates are in `launchagents/`. Update paths and credentials before installing:

```bash
cp launchagents/*.plist ~/Library/LaunchAgents/
# Edit paths in each plist, then:
launchctl load ~/Library/LaunchAgents/com.scout.sequence-api.plist
launchctl load ~/Library/LaunchAgents/com.scout.arq-worker.plist
launchctl load ~/Library/LaunchAgents/com.openclaw.sequence.simple-worker.plist
```
