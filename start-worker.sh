#!/bin/bash
cd "$(dirname "$0")"
exec .venv/bin/python3 -c "
import asyncio
asyncio.set_event_loop(asyncio.new_event_loop())
from arq import run_worker
import sys
sys.path.insert(0, '.')
from src.workers.main import WorkerSettings
run_worker(WorkerSettings)
"
