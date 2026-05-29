"""Email builder with tracking support."""

import re
import html
from typing import Optional

from src.config import get_settings
from src.api.tracking import generate_tracking_pixel_url, wrap_link_for_tracking, generate_unsubscribe_url

settings = get_settings()


def build_tracked_email(
    body: str,
    sent_email_id: str,
    is_html: bool = False,
    enrollment_id: str | None = None,
) -> tuple[str, str]:
    """
    Build an email with open/click tracking, unsubscribe link, and CAN-SPAM footer.

    Args:
        body: Email body (plain text or HTML)
        sent_email_id: ID of the SentEmail record (for tracking)
        is_html: Whether body is already HTML
        enrollment_id: Enrollment ID for generating unsubscribe URL

    Returns:
        Tuple of (html_body, plain_text_body)
    """
    base_url = settings.tracking_base_url

    # Convert to HTML if needed
    if is_html:
        html_body = body
        plain_body = html_to_plain_text(body)
    else:
        html_body = plain_text_to_html(body)
        plain_body = body

    # ── Open/click TRACKING — gated by tracking_enabled (NOT compliance) ──
    # Only the pixel + click-wrapping depend on tracking being on. The
    # unsubscribe + CAN-SPAM footer below are ALWAYS added (Wave 0 fix: they
    # were previously skipped whenever tracking_enabled was false).
    if settings.tracking_enabled:
        html_body = wrap_links_for_tracking(html_body, base_url, sent_email_id)
        tracking_pixel_url = generate_tracking_pixel_url(base_url, sent_email_id)
        tracking_pixel = f'<img src="{tracking_pixel_url}" width="1" height="1" alt="" style="display:none;border:0;width:1px;height:1px;" />'
        if '</body>' in html_body.lower():
            html_body = re.sub(r'(</body>)', f'{tracking_pixel}\\1', html_body, flags=re.IGNORECASE)
        else:
            html_body = f"{html_body}\n{tracking_pixel}"

    # ── ALWAYS: visible unsubscribe + CAN-SPAM postal address (compliance) ──
    # Prefer the one-click HTTPS unsubscribe ONLY when explicitly enabled (a
    # reachable tracking host exists); otherwise use the mailto unsubscribe so we
    # never render a dead link (track.telnyx.com is NXDOMAIN — Wave 0 interim).
    if settings.one_click_unsubscribe_enabled and enrollment_id:
        unsub_target = generate_unsubscribe_url(base_url, enrollment_id)
    else:
        unsub_target = settings.unsubscribe_mailto
    html_body = add_unsubscribe_link(html_body, unsub_target)

    footer = f'<p style="font-size:11px;color:#999;margin-top:20px;">{html.escape(settings.physical_address)}</p>'
    if '</body>' in html_body.lower():
        html_body = re.sub(r'(</body>)', f'{footer}\\1', html_body, flags=re.IGNORECASE)
    else:
        html_body = f"{html_body}\n{footer}"

    # Mirror compliance content into the plain-text part too.
    _unsub_plain = unsub_target[len("mailto:"):] if unsub_target.startswith("mailto:") else unsub_target
    plain_body = f"{plain_body}\n\n--\nUnsubscribe: {_unsub_plain}\n{settings.physical_address}"

    return html_body, plain_body


def plain_text_to_html(text: str) -> str:
    """Convert plain text to simple HTML."""
    # Escape HTML entities
    escaped = html.escape(text)
    
    # Convert URLs to clickable links (must happen before newline conversion)
    url_pattern = r'(https?://[^\s<>"\']+)'
    with_links = re.sub(url_pattern, r'<a href="\1">\1</a>', escaped)
    
    # Convert newlines to <br>
    with_breaks = with_links.replace('\n', '<br>\n')
    
    # Wrap in basic HTML structure
    html_body = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="font-family: Arial, sans-serif; font-size: 14px; line-height: 1.6; color: #333;">
{with_breaks}
</body>
</html>"""
    
    return html_body


def html_to_plain_text(html_content: str) -> str:
    """Convert HTML to plain text."""
    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html_content, 'html.parser')
        return soup.get_text(separator='\n', strip=True)
    except ImportError:
        # Fallback without BeautifulSoup
        text = re.sub(r'<br\s*/?>', '\n', html_content, flags=re.IGNORECASE)
        text = re.sub(r'<[^>]+>', '', text)
        return html.unescape(text)


def wrap_links_for_tracking(html_body: str, base_url: str, sent_email_id: str) -> str:
    """
    Find all links in HTML and wrap them for click tracking.
    
    Handles both href="..." and href='...' formats.
    Skips mailto: links and anchors.
    """
    def replace_link(match):
        full_match = match.group(0)
        url = match.group(1)
        
        # Skip mailto, tel, anchors, and tracking URLs (avoid double-wrapping)
        if url.startswith(('mailto:', 'tel:', '#')) or '/track/' in url:
            return full_match
        
        # Wrap the URL
        tracked_url = wrap_link_for_tracking(base_url, sent_email_id, url)
        return f'href="{tracked_url}"'
    
    # Match href="url" or href='url'
    pattern = r'href=["\']([^"\']+)["\']'
    return re.sub(pattern, replace_link, html_body, flags=re.IGNORECASE)


def add_unsubscribe_link(html_body: str, unsubscribe_url: str) -> str:
    """Add an unsubscribe link to the email footer."""
    unsubscribe_html = f'''
<p style="font-size: 12px; color: #666; margin-top: 30px; border-top: 1px solid #eee; padding-top: 10px;">
    <a href="{unsubscribe_url}" style="color: #666;">Unsubscribe</a>
</p>
'''
    
    if '</body>' in html_body.lower():
        return re.sub(
            r'(</body>)',
            f'{unsubscribe_html}\\1',
            html_body,
            flags=re.IGNORECASE
        )
    else:
        return f"{html_body}\n{unsubscribe_html}"
