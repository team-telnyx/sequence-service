"""
Gmail Service for Sequence Service.

Adapted from Quinn V2's gmail.py with modifications:
- Removed hardcoded Quinn emails
- Generalized for any mailbox via domain-wide delegation
- Simplified for sequence sending use case

Auth: Service account with domain-wide delegation
"""
import base64
import html
import re
import uuid
from datetime import datetime, timezone
from email.message import EmailMessage
from typing import Optional

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from src.config import get_settings

settings = get_settings()


class GmailError(Exception):
    """Base exception for Gmail service errors."""
    pass


class GmailAuthError(GmailError):
    """Authentication or authorization error."""
    pass


class GmailAPIError(GmailError):
    """Gmail API error with status code and message."""
    
    def __init__(self, message: str, status_code: Optional[int] = None, reason: Optional[str] = None):
        self.message = message
        self.status_code = status_code
        self.reason = reason
        super().__init__(f"GmailAPIError [{status_code}]: {message}" if status_code else message)


class GmailService:
    """
    Gmail API integration service with domain-wide delegation.
    
    Can send from any mailbox that has been delegated to the service account.
    """
    
    SCOPES = [
        'https://www.googleapis.com/auth/gmail.send',
        'https://www.googleapis.com/auth/gmail.readonly',
    ]
    
    _instances: dict[str, 'GmailService'] = {}
    
    def __init__(self, inbox: str):
        """
        Initialize Gmail service for a specific inbox.
        
        Args:
            inbox: Email address to send from (must be delegated to service account)
        """
        self.inbox = inbox
        self._service = None
        self._credentials = None
    
    @property
    def service(self):
        """Lazy initialization of Gmail API service."""
        if self._service is None:
            self._service = self._build_service()
        return self._service
    
    def _build_service(self):
        """Build Gmail API service with delegated credentials."""
        if not settings.gmail_service_account_file:
            raise GmailAuthError(
                "gmail_service_account_file not configured. "
                "Set path to service account JSON file."
            )
        
        try:
            credentials = service_account.Credentials.from_service_account_file(
                settings.gmail_service_account_file,
                scopes=self.SCOPES
            )
            delegated_credentials = credentials.with_subject(self.inbox)
            self._credentials = delegated_credentials
            
            return build('gmail', 'v1', credentials=delegated_credentials)
        except FileNotFoundError:
            raise GmailAuthError(
                f"Service account file not found: {settings.gmail_service_account_file}"
            )
        except Exception as e:
            raise GmailAuthError(f"Failed to initialize Gmail service: {e}")
    
    @classmethod
    def get_inbox(cls, email: str) -> 'GmailService':
        """Get or create a GmailService instance for an inbox."""
        if email not in cls._instances:
            cls._instances[email] = cls(inbox=email)
        return cls._instances[email]
    
    def send_email(
        self,
        to: str,
        subject: str,
        body: str,
        thread_id: Optional[str] = None,
        message_id: Optional[str] = None,
        cc: Optional[str] = None,
        reply_to: Optional[str] = None,
        sender_name: Optional[str] = None,
    ) -> dict:
        """
        Send a plain text email.
        
        Args:
            to: Recipient email address
            subject: Email subject
            body: Plain text email body
            thread_id: Gmail thread ID for threading (reply)
            message_id: Message-ID header to reply to
            cc: CC recipients (comma-separated)
            reply_to: Reply-To header
            sender_name: Display name for sender
            
        Returns:
            dict with message_id, thread_id, label_ids
        """
        message = self._build_email_message(
            to=to,
            subject=subject,
            body=body,
            is_html=False,
            cc=cc,
            reply_to=reply_to,
            message_id=message_id,
            sender_name=sender_name,
        )
        
        return self._send_message(message, thread_id)
    
    def send_html_email(
        self,
        to: str,
        subject: str,
        html_body: str,
        thread_id: Optional[str] = None,
        message_id: Optional[str] = None,
        cc: Optional[str] = None,
        reply_to: Optional[str] = None,
        sender_name: Optional[str] = None,
        plain_text_fallback: Optional[str] = None,
        list_unsubscribe: Optional[str] = None,
        one_click: bool = False,
    ) -> dict:
        """Send an HTML email with optional plain text fallback."""
        message = self._build_email_message(
            to=to,
            subject=subject,
            body=html_body,
            is_html=True,
            cc=cc,
            reply_to=reply_to,
            message_id=message_id,
            sender_name=sender_name,
            plain_text_fallback=plain_text_fallback,
            list_unsubscribe=list_unsubscribe,
            one_click=one_click,
        )

        return self._send_message(message, thread_id)
    
    def _build_email_message(
        self,
        to: str,
        subject: str,
        body: str,
        is_html: bool = False,
        cc: Optional[str] = None,
        reply_to: Optional[str] = None,
        message_id: Optional[str] = None,
        sender_name: Optional[str] = None,
        plain_text_fallback: Optional[str] = None,
        list_unsubscribe: Optional[str] = None,
        one_click: bool = False,
    ) -> EmailMessage:
        """Build an EmailMessage object."""
        message = EmailMessage()

        if sender_name:
            message['From'] = f"{sender_name} <{self.inbox}>"
        else:
            message['From'] = self.inbox

        message['To'] = to
        message['Subject'] = subject

        if cc:
            message['Cc'] = cc
        if reply_to:
            message['Reply-To'] = reply_to
        if message_id:
            message['In-Reply-To'] = message_id
            message['References'] = message_id

        # RFC 8058 List-Unsubscribe headers. Only advertise One-Click when a
        # reachable HTTPS unsubscribe endpoint exists (one_click=True); otherwise
        # emit List-Unsubscribe (mailto) WITHOUT the -Post header, so we never
        # claim a one-click endpoint that fails (track.telnyx.com is NXDOMAIN).
        if list_unsubscribe:
            message['List-Unsubscribe'] = list_unsubscribe
            if one_click:
                message['List-Unsubscribe-Post'] = 'List-Unsubscribe=One-Click'

        if is_html:
            unique_token = f'<span style="color: #e0e0e0; font-size: 1px;">{uuid.uuid4()}</span>'
            html_with_token = f"{body}\n{unique_token}"
            
            if plain_text_fallback:
                message.set_content(plain_text_fallback)
            else:
                message.set_content(self._html_to_plain_text(body))
            
            message.add_alternative(html_with_token, subtype='html')
        else:
            message.set_content(body + '\u200B')
        
        return message
    
    def _send_message(self, message: EmailMessage, thread_id: Optional[str] = None) -> dict:
        """Send the email message via Gmail API."""
        try:
            raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode('utf-8')
            body = {'raw': raw_message}
            
            if thread_id:
                body['threadId'] = thread_id
            
            result = self.service.users().messages().send(
                userId='me',
                body=body
            ).execute()
            
            return {
                'message_id': result.get('id'),
                'thread_id': result.get('threadId'),
                'label_ids': result.get('labelIds', []),
            }
            
        except HttpError as e:
            raise GmailAPIError(
                message=f"Failed to send email: {e.reason}",
                status_code=e.resp.status,
                reason=e.reason
            )
    
    @staticmethod
    def _html_to_plain_text(html_content: str) -> str:
        """Convert HTML to plain text (basic conversion)."""
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            return soup.get_text(separator='\n', strip=True)
        except ImportError:
            # Fallback without BeautifulSoup
            text = re.sub(r'<[^>]+>', '', html_content)
            return html.unescape(text)
    
    def check_connection(self) -> bool:
        """Test the Gmail API connection."""
        try:
            profile = self.service.users().getProfile(userId='me').execute()
            return profile.get('emailAddress') == self.inbox
        except HttpError as e:
            raise GmailAPIError(
                message=f"Connection check failed: {e.reason}",
                status_code=e.resp.status,
                reason=e.reason
            )
    
    def list_messages(
        self,
        query: str = "",
        max_results: int = 100,
        label_ids: Optional[list[str]] = None,
    ) -> list[dict]:
        """
        List messages matching a query.
        
        Args:
            query: Gmail search query (e.g., "is:unread", "from:user@example.com")
            max_results: Maximum messages to return
            label_ids: Filter by label IDs (e.g., ["INBOX"])
            
        Returns:
            List of message stubs with id and threadId
        """
        try:
            params = {
                'userId': 'me',
                'maxResults': max_results,
            }
            if query:
                params['q'] = query
            if label_ids:
                params['labelIds'] = label_ids
            
            result = self.service.users().messages().list(**params).execute()
            return result.get('messages', [])
            
        except HttpError as e:
            raise GmailAPIError(
                message=f"Failed to list messages: {e.reason}",
                status_code=e.resp.status,
                reason=e.reason
            )
    
    def get_message(self, message_id: str, format: str = "metadata") -> dict:
        """
        Get a single message.
        
        Args:
            message_id: Gmail message ID
            format: "minimal", "metadata", "full", or "raw"
            
        Returns:
            Message data
        """
        try:
            result = self.service.users().messages().get(
                userId='me',
                id=message_id,
                format=format,
            ).execute()
            return result
            
        except HttpError as e:
            raise GmailAPIError(
                message=f"Failed to get message: {e.reason}",
                status_code=e.resp.status,
                reason=e.reason
            )
    
    def get_thread(self, thread_id: str, format: str = "metadata") -> dict:
        """
        Get all messages in a thread.
        
        Args:
            thread_id: Gmail thread ID
            format: "minimal", "metadata", "full"
            
        Returns:
            Thread data with messages
        """
        try:
            result = self.service.users().threads().get(
                userId='me',
                id=thread_id,
                format=format,
            ).execute()
            return result
            
        except HttpError as e:
            raise GmailAPIError(
                message=f"Failed to get thread: {e.reason}",
                status_code=e.resp.status,
                reason=e.reason
            )
    
    def get_replies_to_threads(self, thread_ids: list[str]) -> list[dict]:
        """
        Get replies to specific threads (messages we sent).
        
        Returns messages that are NOT from our inbox address (i.e., replies from others).
        """
        replies = []
        
        for thread_id in thread_ids:
            try:
                thread = self.get_thread(thread_id, format="metadata")
                messages = thread.get('messages', [])
                
                for msg in messages:
                    # Get the From header
                    headers = {h['name'].lower(): h['value'] for h in msg.get('payload', {}).get('headers', [])}
                    from_addr = headers.get('from', '')
                    
                    # Skip messages from ourselves
                    if self.inbox.lower() in from_addr.lower():
                        continue
                    
                    # This is a reply from someone else
                    # Classify bounce/OOO here (F6): signal_detection reads
                    # reply['is_bounce']/['is_ooo']; without these keys every
                    # mailer-daemon bounce was recorded as a plain REPLY.
                    replies.append({
                        'message_id': msg['id'],
                        'thread_id': msg['threadId'],
                        'from': from_addr,
                        'subject': headers.get('subject', ''),
                        'date': headers.get('date', ''),
                        'snippet': msg.get('snippet', ''),
                        'label_ids': msg.get('labelIds', []),
                        'is_bounce': self.detect_bounce(msg),
                        'is_ooo': self.detect_out_of_office(msg),
                    })
                    
            except GmailAPIError:
                # Skip threads we can't access
                continue
        
        return replies
    
    def detect_bounce(self, message: dict) -> bool:
        """Check if a message is a bounce notification."""
        headers = {h['name'].lower(): h['value'] for h in message.get('payload', {}).get('headers', [])}
        from_addr = headers.get('from', '').lower()
        subject = headers.get('subject', '').lower()
        
        # Common bounce indicators
        bounce_senders = ['mailer-daemon', 'postmaster', 'mail-daemon']
        bounce_subjects = ['undeliverable', 'delivery status', 'delivery failed', 
                          'returned mail', 'failure notice', 'mail delivery failed']
        
        if any(sender in from_addr for sender in bounce_senders):
            return True
        if any(term in subject for term in bounce_subjects):
            return True
        
        return False
    
    def detect_out_of_office(self, message: dict) -> bool:
        """Check if a message is an out-of-office reply."""
        headers = {h['name'].lower(): h['value'] for h in message.get('payload', {}).get('headers', [])}
        subject = headers.get('subject', '').lower()
        snippet = message.get('snippet', '').lower()
        
        # Common OOO indicators
        ooo_terms = ['out of office', 'out of the office', 'automatic reply', 
                     'auto-reply', 'autoreply', 'away from', 'on vacation',
                     'currently out', 'limited access', 'ooo']
        
        text_to_check = subject + ' ' + snippet
        return any(term in text_to_check for term in ooo_terms)
    
    def get_new_inbox_messages(self, since_timestamp: Optional[datetime] = None) -> list[dict]:
        """
        Get new messages in inbox, optionally since a timestamp.
        
        Args:
            since_timestamp: Only get messages after this time
            
        Returns:
            List of message data with headers
        """
        query = "in:inbox"
        if since_timestamp:
            # Gmail uses epoch seconds for after: query
            epoch = int(since_timestamp.timestamp())
            query += f" after:{epoch}"
        
        message_stubs = self.list_messages(query=query, max_results=50)
        
        messages = []
        for stub in message_stubs:
            try:
                msg = self.get_message(stub['id'], format="metadata")
                headers = {h['name'].lower(): h['value'] for h in msg.get('payload', {}).get('headers', [])}
                
                messages.append({
                    'message_id': msg['id'],
                    'thread_id': msg['threadId'],
                    'from': headers.get('from', ''),
                    'to': headers.get('to', ''),
                    'subject': headers.get('subject', ''),
                    'date': headers.get('date', ''),
                    'in_reply_to': headers.get('in-reply-to', ''),
                    'references': headers.get('references', ''),
                    'snippet': msg.get('snippet', ''),
                    'label_ids': msg.get('labelIds', []),
                    'is_bounce': self.detect_bounce(msg),
                    'is_ooo': self.detect_out_of_office(msg),
                })
            except GmailAPIError:
                continue
        
        return messages


async def send_test_email(
    to: str,
    from_email: str,
    subject: str = "Test from Sequence Service",
    body: str = "This is a test email from the Sequence Service.",
    sender_name: Optional[str] = None,
) -> dict:
    """
    Send a test email.
    
    Args:
        to: Recipient email
        from_email: Sender email (must be delegated)
        subject: Email subject
        body: Email body
        sender_name: Display name
        
    Returns:
        Send result dict
    """
    gmail = GmailService.get_inbox(from_email)
    return gmail.send_email(
        to=to,
        subject=subject,
        body=body,
        sender_name=sender_name,
    )
