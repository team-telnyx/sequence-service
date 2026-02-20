/**
 * Gmail Integration Service
 * 
 * Ported from quinn-v2/services/gmail.py with modifications:
 * - Remove QUINN_EMAILS constant 
 * - Remove INBOX_MAP
 * - Generalize is_from_quinn → is_from_sender
 * - TypeScript implementation
 * - GMAIL_ENABLED=false by default (stub mode)
 */

import { PrismaClient, SignalType } from '@prisma/client';
import { google } from 'googleapis';

const prisma = new PrismaClient();

export interface GmailMessage {
  id: string;
  threadId: string;
  snippet: string;
  payload: any;
  internalDate: string;
}

export interface ParsedEmail {
  messageId: string;
  threadId: string;
  subject: string;
  from: string;
  to: string;
  date: Date;
  body: string;
  isReply: boolean;
  replyToMessageId?: string;
  signalType?: SignalType;
}

export class GmailService {
  private oauth2Client: any;
  private gmail: any;

  constructor() {
    if (process.env.GMAIL_ENABLED === 'true') {
      // Initialize Google OAuth2 client
      this.oauth2Client = new google.auth.OAuth2(
        process.env.GOOGLE_CLIENT_ID,
        process.env.GOOGLE_CLIENT_SECRET,
        process.env.GOOGLE_REDIRECT_URI
      );

      this.gmail = google.gmail({ version: 'v1', auth: this.oauth2Client });
    }
  }

  /**
   * Set access credentials for a mailbox
   */
  async setCredentials(mailboxId: string): Promise<boolean> {
    if (process.env.GMAIL_ENABLED !== 'true') {
      console.log(`[STUB] Setting credentials for mailbox: ${mailboxId}`);
      return true;
    }

    try {
      const mailbox = await prisma.mailbox.findUnique({
        where: { id: mailboxId },
        select: {
          accessToken: true,
          refreshToken: true,
          tokenExpiresAt: true,
        },
      });

      if (!mailbox || !mailbox.refreshToken) {
        throw new Error('Mailbox not found or missing refresh token');
      }

      this.oauth2Client.setCredentials({
        access_token: mailbox.accessToken,
        refresh_token: mailbox.refreshToken,
        expiry_date: mailbox.tokenExpiresAt?.getTime(),
      });

      // Refresh token if expired
      if (!mailbox.accessToken || (mailbox.tokenExpiresAt && mailbox.tokenExpiresAt < new Date())) {
        const { credentials } = await this.oauth2Client.refreshAccessToken();
        
        // Update stored tokens
        await prisma.mailbox.update({
          where: { id: mailboxId },
          data: {
            accessToken: credentials.access_token,
            tokenExpiresAt: new Date(credentials.expiry_date),
          },
        });

        this.oauth2Client.setCredentials(credentials);
      }

      return true;
    } catch (error) {
      console.error(`Failed to set Gmail credentials for mailbox ${mailboxId}:`, error);
      return false;
    }
  }

  /**
   * Send an email via Gmail API
   */
  async sendEmail(params: {
    mailboxId: string;
    to: string;
    toName?: string;
    subject: string;
    body: string;
    replyToMessageId?: string;
  }): Promise<{ messageId: string; success: boolean }> {
    if (process.env.GMAIL_ENABLED !== 'true') {
      // Stub mode
      const messageId = `stub-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
      console.log(`[STUB] Sending email via Gmail: ${params.subject} to ${params.to}`);
      return { messageId, success: true };
    }

    try {
      const credentialsSet = await this.setCredentials(params.mailboxId);
      if (!credentialsSet) {
        throw new Error('Failed to set Gmail credentials');
      }

      const mailbox = await prisma.mailbox.findUnique({
        where: { id: params.mailboxId },
        select: { email: true, displayName: true },
      });

      if (!mailbox) {
        throw new Error('Mailbox not found');
      }

      // Construct email message
      const messageParts = [
        `To: ${params.toName ? `"${params.toName}" <${params.to}>` : params.to}`,
        `From: ${mailbox.displayName ? `"${mailbox.displayName}" <${mailbox.email}>` : mailbox.email}`,
        `Subject: ${params.subject}`,
        'Content-Type: text/html; charset=UTF-8',
        'MIME-Version: 1.0',
        '',
        params.body
      ];

      if (params.replyToMessageId) {
        messageParts.splice(3, 0, `In-Reply-To: <${params.replyToMessageId}>`);
      }

      const message = messageParts.join('\n');
      const encodedMessage = Buffer.from(message).toString('base64')
        .replace(/\+/g, '-')
        .replace(/\//g, '_')
        .replace(/=+$/, '');

      const response = await this.gmail.users.messages.send({
        userId: 'me',
        requestBody: {
          raw: encodedMessage,
        },
      });

      return { messageId: response.data.id, success: true };
    } catch (error) {
      console.error(`Failed to send email via Gmail:`, error);
      return { messageId: '', success: false };
    }
  }

  /**
   * Check for new messages and signals
   */
  async checkForSignals(mailboxId: string, lastCheckTime?: Date): Promise<ParsedEmail[]> {
    if (process.env.GMAIL_ENABLED !== 'true' && process.env.SCOUT_STUB_MODE !== 'true') {
      return [];
    }

    if (process.env.SCOUT_STUB_MODE === 'true') {
      // Return stub signals for testing
      return this.generateStubSignals(mailboxId);
    }

    try {
      const credentialsSet = await this.setCredentials(mailboxId);
      if (!credentialsSet) {
        return [];
      }

      // Build Gmail API query
      let query = 'in:inbox';
      if (lastCheckTime) {
        const timestamp = Math.floor(lastCheckTime.getTime() / 1000);
        query += ` after:${timestamp}`;
      }

      const response = await this.gmail.users.messages.list({
        userId: 'me',
        q: query,
        maxResults: 50,
      });

      if (!response.data.messages) {
        return [];
      }

      const parsedEmails: ParsedEmail[] = [];

      for (const message of response.data.messages) {
        try {
          const fullMessage = await this.gmail.users.messages.get({
            userId: 'me',
            id: message.id,
          });

          const parsed = await this.parseGmailMessage(fullMessage.data, mailboxId);
          if (parsed) {
            parsedEmails.push(parsed);
          }
        } catch (error) {
          console.error(`Failed to parse message ${message.id}:`, error);
        }
      }

      return parsedEmails;
    } catch (error) {
      console.error(`Failed to check for Gmail signals:`, error);
      return [];
    }
  }

  /**
   * Parse a Gmail message and determine signal type
   */
  private async parseGmailMessage(message: GmailMessage, mailboxId: string): Promise<ParsedEmail | null> {
    try {
      const headers = message.payload.headers || [];
      const getHeader = (name: string) => headers.find((h: any) => h.name.toLowerCase() === name.toLowerCase())?.value || '';

      const subject = getHeader('subject');
      const from = getHeader('from');
      const to = getHeader('to');
      const date = new Date(parseInt(message.internalDate));
      const messageId = getHeader('message-id');
      const inReplyTo = getHeader('in-reply-to');
      const references = getHeader('references');

      // Extract email body
      const body = this.extractEmailBody(message.payload);

      // Determine if this is a reply to one of our sent emails
      const isReply = await this.isReplyToSentEmail(mailboxId, inReplyTo, references, from);
      
      // Detect signal type
      let signalType: SignalType | undefined;
      
      if (isReply) {
        if (this.isOutOfOfficeReply(subject, body)) {
          signalType = 'OUT_OF_OFFICE';
        } else if (this.isBounceMessage(from, subject, body)) {
          signalType = 'BOUNCE';
        } else if (this.isUnsubscribeMessage(subject, body)) {
          signalType = 'UNSUBSCRIBE';
        } else {
          signalType = 'REPLY';
        }
      }

      return {
        messageId: message.id,
        threadId: message.threadId,
        subject,
        from,
        to,
        date,
        body,
        isReply,
        replyToMessageId: inReplyTo,
        signalType,
      };
    } catch (error) {
      console.error('Failed to parse Gmail message:', error);
      return null;
    }
  }

  /**
   * Extract plain text body from Gmail message payload
   */
  private extractEmailBody(payload: any): string {
    if (payload.body?.data) {
      return Buffer.from(payload.body.data, 'base64').toString('utf-8');
    }

    if (payload.parts) {
      for (const part of payload.parts) {
        if (part.mimeType === 'text/plain' && part.body?.data) {
          return Buffer.from(part.body.data, 'base64').toString('utf-8');
        }
        
        // Recursively check nested parts
        if (part.parts) {
          const nestedBody = this.extractEmailBody(part);
          if (nestedBody) return nestedBody;
        }
      }
    }

    return '';
  }

  /**
   * Check if message is a reply to one of our sent emails
   * Generalized from is_from_quinn to is_from_sender
   */
  private async isReplyToSentEmail(mailboxId: string, inReplyTo: string, references: string, fromEmail: string): Promise<boolean> {
    if (!inReplyTo && !references) {
      return false;
    }

    // Check if we have a sent email with matching message ID
    const messageIds = [inReplyTo, ...(references ? references.split(/\s+/) : [])].filter(Boolean);
    
    if (messageIds.length === 0) {
      return false;
    }

    const sentEmail = await prisma.sentEmail.findFirst({
      where: {
        mailboxId,
        messageId: {
          in: messageIds,
        },
      },
    });

    return !!sentEmail;
  }

  /**
   * Check if message is an out-of-office reply
   */
  private isOutOfOfficeReply(subject: string, body: string): boolean {
    const oofPatterns = [
      /out of office/i,
      /away from office/i,
      /vacation/i,
      /holiday/i,
      /absence/i,
      /unavailable/i,
      /automatic reply/i,
      /auto-reply/i,
    ];

    const text = `${subject} ${body}`;
    return oofPatterns.some(pattern => pattern.test(text));
  }

  /**
   * Check if message is a bounce/delivery failure
   */
  private isBounceMessage(from: string, subject: string, body: string): boolean {
    const bounceFromPatterns = [
      /mailer-daemon/i,
      /postmaster/i,
      /noreply/i,
      /no-reply/i,
    ];

    const bounceSubjectPatterns = [
      /delivery.*fail/i,
      /undeliverable/i,
      /bounce/i,
      /returned mail/i,
      /delivery status notification/i,
    ];

    const bounceBodyPatterns = [
      /delivery.*fail/i,
      /recipient.*unknown/i,
      /user.*not.*found/i,
      /mailbox.*full/i,
      /message.*rejected/i,
    ];

    const text = `${subject} ${body}`;
    
    return bounceFromPatterns.some(pattern => pattern.test(from)) ||
           bounceSubjectPatterns.some(pattern => pattern.test(subject)) ||
           bounceBodyPatterns.some(pattern => pattern.test(text));
  }

  /**
   * Check if message is an unsubscribe request
   */
  private isUnsubscribeMessage(subject: string, body: string): boolean {
    const unsubPatterns = [
      /unsubscribe/i,
      /remove.*list/i,
      /stop.*email/i,
      /opt.*out/i,
    ];

    const text = `${subject} ${body}`;
    return unsubPatterns.some(pattern => pattern.test(text));
  }

  /**
   * Generate stub signals for testing
   */
  private generateStubSignals(mailboxId: string): ParsedEmail[] {
    const signals: ParsedEmail[] = [];
    
    // Randomly generate signals for demo
    if (Math.random() < 0.3) { // 30% chance
      const signalTypes: SignalType[] = ['REPLY', 'BOUNCE', 'OUT_OF_OFFICE', 'UNSUBSCRIBE'];
      const randomType = signalTypes[Math.floor(Math.random() * signalTypes.length)];
      
      signals.push({
        messageId: `stub-signal-${Date.now()}`,
        threadId: `thread-${Math.random().toString(36).substr(2, 9)}`,
        subject: this.getStubSubjectForSignalType(randomType),
        from: 'test@example.com',
        to: 'sender@example.com',
        date: new Date(),
        body: this.getStubBodyForSignalType(randomType),
        isReply: true,
        signalType: randomType,
      });
    }

    return signals;
  }

  private getStubSubjectForSignalType(signalType: SignalType): string {
    switch (signalType) {
      case 'OUT_OF_OFFICE':
        return 'Out of Office Auto-Reply';
      case 'BOUNCE':
        return 'Delivery Status Notification (Failure)';
      case 'UNSUBSCRIBE':
        return 'Please remove me from your list';
      case 'REPLY':
        return 'Re: Your email';
      default:
        return 'Test Signal';
    }
  }

  private getStubBodyForSignalType(signalType: SignalType): string {
    switch (signalType) {
      case 'OUT_OF_OFFICE':
        return 'I am currently out of the office and will return on Monday.';
      case 'BOUNCE':
        return 'The following message could not be delivered to the recipient.';
      case 'UNSUBSCRIBE':
        return 'Please unsubscribe me from this mailing list.';
      case 'REPLY':
        return 'Thanks for your email. I will get back to you soon.';
      default:
        return 'Test signal message body.';
    }
  }
}

export const gmailService = new GmailService();