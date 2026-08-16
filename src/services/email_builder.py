"""Email builder — CAN-SPAM compliance footer + HTML/plain conversion.

SV2-044: open-pixel and click-wrap tracking code DELETED (base64-as-signature
was a real defect — `encode_tracking_id`/`decode_tracking_id` are gone, along
with the `/track/open` and `/track/click` redirect endpoints). Delivery events
now come from the Telnyx Email API webhook (sent/delivered/bounced/opened/
clicked/suppressed). The visible unsubscribe link + CAN-SPAM postal address
remain (compliance requirement, not tracking).
"""

import re
import html

from src.config import get_settings

settings = get_settings()


def build_tracked_email(
    body: str,
    sent_email_id: str,
    is_html: bool = False,
    enrollment_id: str | None = None,
) -> tuple[str, str]:
    """Build an email with unsubscribe link + CAN-SPAM footer.

    The ``sent_email_id`` and ``enrollment_id`` params are retained for call-site
    compatibility but are no longer used for tracking pixel/redirect URL generation
    (that machinery was deleted in SV2-044). Delivery events come from the
    Email API webhook.

    Returns (html_body, plain_text_body).
    """
    if is_html:
        html_body = body
        plain_body = html_to_plain_text(body)
    else:
        html_body = plain_text_to_html(body)
        plain_body = body

    unsub_target = settings.unsubscribe_mailto
    html_body = add_unsubscribe_link(html_body, unsub_target)

    footer = f'<p style="font-size:11px;color:#999;margin-top:20px;">{html.escape(settings.physical_address)}</p>'
    if "</body>" in html_body.lower():
        html_body = re.sub(r"(</body>)", f"{footer}\\1", html_body, flags=re.IGNORECASE)
    else:
        html_body = f"{html_body}\n{footer}"

    _unsub_plain = (
        unsub_target[len("mailto:") :] if unsub_target.startswith("mailto:") else unsub_target
    )
    plain_body = f"{plain_body}\n\n--\nUnsubscribe: {_unsub_plain}\n{settings.physical_address}"

    return html_body, plain_body


def plain_text_to_html(text: str) -> str:
    """Convert plain text to simple HTML."""
    escaped = html.escape(text)

    url_pattern = r'(https?://[^\s<>"\']+)'
    with_links = re.sub(url_pattern, r'<a href="\1">\1</a>', escaped)

    with_breaks = with_links.replace("\n", "<br>\n")

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

        soup = BeautifulSoup(html_content, "html.parser")
        return soup.get_text(separator="\n", strip=True)
    except ImportError:
        text = re.sub(r"<br\s*/?>", "\n", html_content, flags=re.IGNORECASE)
        text = re.sub(r"<[^>]+>", "", text)
        return html.unescape(text)


def add_unsubscribe_link(html_body: str, unsubscribe_url: str) -> str:
    """Add an unsubscribe link to the email footer."""
    unsubscribe_html = f'''
<p style="font-size: 12px; color: #666; margin-top: 30px; border-top: 1px solid #eee; padding-top: 10px;">
    <a href="{unsubscribe_url}" style="color: #666;">Unsubscribe</a>
</p>
'''

    if "</body>" in html_body.lower():
        return re.sub(r"(</body>)", f"{unsubscribe_html}\\1", html_body, flags=re.IGNORECASE)
    else:
        return f"{html_body}\n{unsubscribe_html}"
