#!/usr/bin/env python3
"""
Scout → Sequence Service Enrollment Bridge

Polls scout DB for newly enrolled prospects (status='enrolled', no corresponding
sequence_service enrollment) and creates sequence_service enrollments via API.

This is the bridge between run_autopilot_cycle.py (writes to scout DB) and the
arq worker (reads from sequence_service DB via enrollment steps).

Runs continuously with a 60-second poll interval.
"""
import asyncio
import os
import sys
import logging
import json
import httpx

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger('simple_worker')

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

SCOUT_DB_URL = os.getenv("SCOUT_DATABASE_URL", "postgresql://kevinward@localhost:5432/scout")
SEQ_SERVICE_URL = os.getenv("SEQUENCE_SERVICE_URL", "http://localhost:8001")
SEQ_API_KEY = os.getenv("SEQUENCE_SERVICE_API_KEY", "scout-api-key-prod")
DEFAULT_SEQUENCE_ID = os.getenv("DEFAULT_SEQUENCE_ID", "03510577-09d1-4879-87a0-7b8b76c2a60b")
SEQ_DB_URL = "postgresql://kevinward@localhost:5432/sequence_service"

POLL_INTERVAL = 60  # seconds

# TLD → timezone mapping for APAC
APAC_TLD_TIMEZONES = {
    '.com.au': 'Australia/Sydney',
    '.net.au': 'Australia/Sydney',
    '.org.au': 'Australia/Sydney',
    '.co.nz': 'Pacific/Auckland',
    '.nz': 'Pacific/Auckland',
    '.jp': 'Asia/Tokyo',
    '.sg': 'Asia/Singapore',
    '.my': 'Asia/Kuala_Lumpur',
    '.ph': 'Asia/Manila',
    '.id': 'Asia/Jakarta',
    '.hk': 'Asia/Hong_Kong',
    '.cn': 'Asia/Shanghai',
    '.tw': 'Asia/Taipei',
    '.kr': 'Asia/Seoul',
    '.th': 'Asia/Bangkok',
    '.vn': 'Asia/Ho_Chi_Minh',
    '.in': 'Asia/Kolkata',
}

# Non-APAC TLDs to exclude
NON_APAC_TLDS = {
    '.uk', '.co.uk', '.org.uk', '.me.uk',
    '.de', '.fr', '.it', '.es', '.nl', '.be', '.ch', '.at',
    '.no', '.se', '.dk', '.fi', '.pl', '.cz', '.pt', '.ie',
    '.br', '.mx', '.ar', '.co', '.cl',
    '.ca',  # Canada — not APAC
    '.ru', '.ua',
    '.za',  # South Africa
}

DEFAULT_APAC_TZ = 'Australia/Sydney'


def get_timezone_for_email(email: str) -> str | None:
    """Return IANA timezone for an email based on TLD, or None if non-APAC."""
    domain = email.split('@')[-1].lower() if '@' in email else email.lower()
    # Check non-APAC exclusions first (longer TLDs first)
    for tld in sorted(NON_APAC_TLDS, key=len, reverse=True):
        if domain.endswith(tld):
            return None  # Exclude
    # Check APAC TLD mapping
    for tld, tz in sorted(APAC_TLD_TIMEZONES.items(), key=lambda x: len(x[0]), reverse=True):
        if domain.endswith(tld):
            return tz
    # Unknown TLD (.com, .net, .org, etc.) — include with default APAC timezone
    return DEFAULT_APAC_TZ


async def get_unenrolled_prospects(scout_pool) -> list:
    """Get prospects enrolled in scout DB but not yet in sequence_service."""
    # Get all emails enrolled in sequence_service
    import asyncpg
    seq_pool = await asyncpg.create_pool(SEQ_DB_URL, min_size=1, max_size=3)
    try:
        enrolled_emails = set(
            r['contact_email']
            for r in await seq_pool.fetch(
                "SELECT contact_email FROM sequence_enrollments WHERE sequence_id = $1",
                DEFAULT_SEQUENCE_ID
            )
        )
    finally:
        await seq_pool.close()

    # Find scout DB sequences not yet enrolled
    rows = await scout_pool.fetch("""
        SELECT
            p.id as prospect_id,
            p.email,
            p.first_name,
            p.last_name,
            a.company_name,
            s.id as sequence_id,
            s.cadence_config,
            s.created_at
        FROM sequences s
        JOIN prospects p ON s.prospect_id = p.id
        JOIN accounts a ON p.account_id = a.id
        WHERE s.status = 'active'
          AND p.email IS NOT NULL
          AND p.email != ''
        ORDER BY s.created_at DESC
        LIMIT 50
    """)

    unenrolled = []
    for row in rows:
        email = row['email']
        if email not in enrolled_emails:
            prospect = dict(row)
            # Extract custom email content from cadence_config step 1 if it exists
            try:
                cadence = json.loads(prospect['cadence_config']) if prospect['cadence_config'] else {}
                steps = cadence.get('steps', [])
                if steps:
                    step1 = steps[0]
                    prospect['custom_subject'] = step1.get('subject', '')
                    prospect['custom_body'] = step1.get('body', '')
                else:
                    prospect['custom_subject'] = None
                    prospect['custom_body'] = None
            except Exception:
                prospect['custom_subject'] = None
                prospect['custom_body'] = None
            unenrolled.append(prospect)

    return unenrolled


async def enroll_in_sequence_service(prospect: dict) -> bool:
    """POST enrollment to sequence_service API."""
    # Check APAC filter
    timezone = get_timezone_for_email(prospect['email'])
    if timezone is None:
        logger.info(f"Skipping non-APAC contact: {prospect['email']}")
        return True  # Not an error, just filtered

    contact_name = ' '.join(filter(None, [
        prospect.get('first_name', ''),
        prospect.get('last_name', '')
    ])).strip() or prospect.get('email', '')

    payload = {
        "sequence_id": DEFAULT_SEQUENCE_ID,
        "contact_email": prospect['email'],
        "contact_name": contact_name,
        "timezone": timezone,
    }

    # Pass Scout-composed first-touch content if available
    if prospect.get('custom_subject') and prospect.get('custom_body'):
        payload["email_subject"] = prospect['custom_subject']
        payload["email_body"] = prospect['custom_body']

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"{SEQ_SERVICE_URL}/api/enrollments/",
                json=payload,
                headers={"X-API-Key": SEQ_API_KEY},
            )
            if resp.status_code == 201:
                data = resp.json()
                enrollment_id = data.get('data', {}).get('id', '?')
                logger.info(
                    f"Enrolled {prospect['email']} ({prospect['company_name']}) "
                    f"→ enrollment {enrollment_id}"
                )
                return True
            elif resp.status_code == 409:
                # Already enrolled — not an error
                logger.debug(f"Already enrolled: {prospect['email']}")
                return True
            else:
                logger.error(
                    f"Failed to enroll {prospect['email']}: "
                    f"HTTP {resp.status_code} — {resp.text[:200]}"
                )
                return False
    except Exception as e:
        logger.error(f"Error enrolling {prospect['email']}: {e}")
        return False


async def run_cycle(scout_pool) -> dict:
    """Run one enrollment bridge cycle."""
    stats = {'checked': 0, 'enrolled': 0, 'failed': 0}

    try:
        unenrolled = await get_unenrolled_prospects(scout_pool)
        stats['checked'] = len(unenrolled)

        if not unenrolled:
            return stats

        logger.info(f"Found {len(unenrolled)} prospects needing sequence_service enrollment")

        for prospect in unenrolled:
            ok = await enroll_in_sequence_service(prospect)
            if ok:
                stats['enrolled'] += 1
            else:
                stats['failed'] += 1

            # Small delay between enrollments to avoid overwhelming the API
            await asyncio.sleep(0.5)

    except Exception as e:
        logger.error(f"Bridge cycle error: {e}")

    return stats


async def main():
    import asyncpg

    logger.info(
        f"Simple Worker (enrollment bridge) starting — "
        f"poll every {POLL_INTERVAL}s | "
        f"sequence={DEFAULT_SEQUENCE_ID}"
    )

    scout_pool = await asyncpg.create_pool(SCOUT_DB_URL, min_size=1, max_size=3)

    try:
        while True:
            stats = await run_cycle(scout_pool)
            if stats['checked'] > 0:
                logger.info(
                    f"Bridge cycle: checked={stats['checked']} "
                    f"enrolled={stats['enrolled']} failed={stats['failed']}"
                )
            await asyncio.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        logger.info("Shutting down")
    finally:
        await scout_pool.close()


if __name__ == "__main__":
    asyncio.run(main())
