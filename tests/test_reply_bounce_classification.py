"""Poller bounce mislabel (audit F6 follow-up).

get_replies_to_threads() built reply dicts WITHOUT is_bounce/is_ooo, so
signal_detection always fell through to SignalType.REPLY — mailer-daemon bounces
were recorded as REPLY (154/168 of the live backlog). detect_bounce/
detect_out_of_office already existed; this path just never called them.
"""
import sys, os
from unittest.mock import MagicMock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.services.gmail import GmailService


def _msg(msg_id, from_addr, subject="hi", snippet="hello"):
    return {
        "id": msg_id, "threadId": "t-" + msg_id,
        "snippet": snippet,
        "payload": {"headers": [
            {"name": "From", "value": from_addr},
            {"name": "Subject", "value": subject},
        ]},
    }


def _gmail_with_thread(messages):
    g = GmailService.__new__(GmailService)   # bypass __init__/auth
    g.inbox = "quinn.c@telnyx.com"
    g.get_thread = MagicMock(return_value={"messages": messages})
    return g


def test_mailer_daemon_reply_flagged_is_bounce():
    g = _gmail_with_thread([_msg("m1", "mailer-daemon@googlemail.com",
                                 subject="Delivery Status Notification (Failure)")])
    replies = g.get_replies_to_threads(["t-m1"])
    assert len(replies) == 1
    assert replies[0]["is_bounce"] is True
    assert replies[0]["is_ooo"] is False


def test_ooo_reply_flagged_is_ooo():
    g = _gmail_with_thread([_msg("m2", "vp@acme.com",
                                 subject="Automatic reply: out of office")])
    replies = g.get_replies_to_threads(["t-m2"])
    assert replies[0]["is_ooo"] is True
    assert replies[0]["is_bounce"] is False


def test_genuine_reply_neither():
    g = _gmail_with_thread([_msg("m3", "vp@acme.com",
                                 subject="Re: your email", snippet="sure, let's talk")])
    replies = g.get_replies_to_threads(["t-m3"])
    assert replies[0]["is_bounce"] is False
    assert replies[0]["is_ooo"] is False


if __name__ == "__main__":
    import traceback
    p = f = 0
    for n, fn in sorted(globals().items()):
        if n.startswith("test_") and callable(fn):
            try: fn(); print(f"PASS {n}"); p += 1
            except Exception: print(f"FAIL {n}"); traceback.print_exc(); f += 1
    print(f"\n{p} passed, {f} failed"); sys.exit(1 if f else 0)
