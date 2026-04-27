#!/usr/bin/env python3
"""
ARQ Worker startup script — Python 3.10+ compatible.

Works around the asyncio.get_event_loop() deprecation in Python 3.10+
which causes arq's run_worker() to fail without an active event loop.
"""
import asyncio
import sys
import os

# Ensure an event loop exists before arq tries to get one
asyncio.set_event_loop(asyncio.new_event_loop())

# Add service root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from arq import run_worker
from src.workers.main import WorkerSettings

if __name__ == "__main__":
    run_worker(WorkerSettings)
