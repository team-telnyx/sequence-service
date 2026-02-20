import { PrismaClient, MailboxStatus } from '@prisma/client';

const prisma = new PrismaClient();

export interface MailboxSelectionOptions {
  tenantId: string;
  excludeMailboxIds?: string[];
  requireMinimumSends?: number;
}

export interface SelectedMailbox {
  id: string;
  email: string;
  displayName?: string;
  weight: number;
  dailySendLimit: number;
  sentToday: number;
  availableSends: number;
}

export class MailboxRotationService {
  /**
   * Select a mailbox for sending using weighted selection
   */
  async selectMailbox(options: MailboxSelectionOptions): Promise<SelectedMailbox | null> {
    const { tenantId, excludeMailboxIds = [], requireMinimumSends = 1 } = options;

    // Get all active mailboxes for the tenant
    const mailboxes = await prisma.mailbox.findMany({
      where: {
        tenantId,
        status: MailboxStatus.ACTIVE,
        id: {
          notIn: excludeMailboxIds,
        },
        // Only include mailboxes that haven't reached their daily limit
        sentToday: {
          lt: prisma.mailbox.fields.dailySendLimit,
        },
      },
      select: {
        id: true,
        email: true,
        displayName: true,
        weight: true,
        dailySendLimit: true,
        sentToday: true,
      },
    });

    if (mailboxes.length === 0) {
      return null;
    }

    // Calculate available sends and filter by minimum requirement
    const availableMailboxes = mailboxes
      .map(mailbox => ({
        ...mailbox,
        availableSends: mailbox.dailySendLimit - mailbox.sentToday,
      }))
      .filter(mailbox => mailbox.availableSends >= requireMinimumSends);

    if (availableMailboxes.length === 0) {
      return null;
    }

    // Perform weighted selection
    const selectedMailbox = this.performWeightedSelection(availableMailboxes);
    return selectedMailbox;
  }

  /**
   * Perform weighted random selection
   */
  private performWeightedSelection(mailboxes: SelectedMailbox[]): SelectedMailbox {
    // Calculate total weight, adjusting for availability
    const totalWeight = mailboxes.reduce((sum, mailbox) => {
      // Boost weight based on available capacity
      const availabilityBoost = mailbox.availableSends / mailbox.dailySendLimit;
      return sum + (mailbox.weight * availabilityBoost);
    }, 0);

    // Generate random number
    const random = Math.random() * totalWeight;
    
    // Find the selected mailbox
    let currentWeight = 0;
    for (const mailbox of mailboxes) {
      const availabilityBoost = mailbox.availableSends / mailbox.dailySendLimit;
      currentWeight += (mailbox.weight * availabilityBoost);
      
      if (random <= currentWeight) {
        return mailbox;
      }
    }

    // Fallback to the first mailbox (shouldn't happen)
    return mailboxes[0];
  }

  /**
   * Reserve a send slot for a mailbox
   */
  async reserveSend(mailboxId: string): Promise<boolean> {
    try {
      const result = await prisma.mailbox.updateMany({
        where: {
          id: mailboxId,
          sentToday: {
            lt: prisma.mailbox.fields.dailySendLimit,
          },
        },
        data: {
          sentToday: {
            increment: 1,
          },
        },
      });

      return result.count > 0;
    } catch (error) {
      console.error('Failed to reserve send slot:', error);
      return false;
    }
  }

  /**
   * Release a reserved send slot (in case of send failure)
   */
  async releaseSend(mailboxId: string): Promise<boolean> {
    try {
      const result = await prisma.mailbox.updateMany({
        where: {
          id: mailboxId,
          sentToday: {
            gt: 0,
          },
        },
        data: {
          sentToday: {
            decrement: 1,
          },
        },
      });

      return result.count > 0;
    } catch (error) {
      console.error('Failed to release send slot:', error);
      return false;
    }
  }

  /**
   * Get mailbox statistics for a tenant
   */
  async getMailboxStats(tenantId: string) {
    const mailboxes = await prisma.mailbox.findMany({
      where: { tenantId },
      select: {
        id: true,
        email: true,
        displayName: true,
        status: true,
        weight: true,
        dailySendLimit: true,
        sentToday: true,
      },
    });

    const stats = {
      total: mailboxes.length,
      active: mailboxes.filter(m => m.status === MailboxStatus.ACTIVE).length,
      paused: mailboxes.filter(m => m.status === MailboxStatus.PAUSED).length,
      outOfOffice: mailboxes.filter(m => m.status === MailboxStatus.OUT_OF_OFFICE).length,
      bounced: mailboxes.filter(m => m.status === MailboxStatus.BOUNCED).length,
      disabled: mailboxes.filter(m => m.status === MailboxStatus.DISABLED).length,
      totalDailyLimit: mailboxes.reduce((sum, m) => sum + m.dailySendLimit, 0),
      totalSentToday: mailboxes.reduce((sum, m) => sum + m.sentToday, 0),
      availableCapacity: mailboxes
        .filter(m => m.status === MailboxStatus.ACTIVE)
        .reduce((sum, m) => sum + (m.dailySendLimit - m.sentToday), 0),
    };

    return {
      stats,
      mailboxes: mailboxes.map(m => ({
        ...m,
        availableSends: m.dailySendLimit - m.sentToday,
        utilizationRate: (m.sentToday / m.dailySendLimit * 100).toFixed(1),
      })),
    };
  }

  /**
   * Reset daily send counters for all mailboxes
   */
  async resetDailySendCounters(tenantId?: string): Promise<number> {
    const where = tenantId ? { tenantId } : {};
    
    const result = await prisma.mailbox.updateMany({
      where,
      data: {
        sentToday: 0,
      },
    });

    return result.count;
  }

  /**
   * Check circuit breaker conditions
   */
  async checkCircuitBreaker(tenantId: string, timeWindow = 24): Promise<{
    shouldTrigger: boolean;
    bounceRate: number;
    totalEmails: number;
    bounces: number;
  }> {
    const cutoffTime = new Date(Date.now() - (timeWindow * 60 * 60 * 1000));

    // Get total emails sent in the time window
    const totalEmails = await prisma.sentEmail.count({
      where: {
        mailbox: {
          tenantId,
        },
        sentAt: {
          gte: cutoffTime,
        },
      },
    });

    if (totalEmails === 0) {
      return {
        shouldTrigger: false,
        bounceRate: 0,
        totalEmails: 0,
        bounces: 0,
      };
    }

    // Get bounce signals in the same time window
    const bounces = await prisma.signal.count({
      where: {
        type: 'BOUNCE',
        mailbox: {
          tenantId,
        },
        detectedAt: {
          gte: cutoffTime,
        },
      },
    });

    const bounceRate = (bounces / totalEmails) * 100;
    const shouldTrigger = bounceRate > 2.0; // 2% threshold

    return {
      shouldTrigger,
      bounceRate: parseFloat(bounceRate.toFixed(2)),
      totalEmails,
      bounces,
    };
  }

  /**
   * Pause mailboxes due to circuit breaker
   */
  async triggerCircuitBreaker(tenantId: string, reason = 'High bounce rate detected'): Promise<number> {
    const result = await prisma.mailbox.updateMany({
      where: {
        tenantId,
        status: MailboxStatus.ACTIVE,
      },
      data: {
        status: MailboxStatus.PAUSED,
      },
    });

    // TODO: Create webhook event for circuit breaker trigger
    
    return result.count;
  }
}

export const mailboxRotationService = new MailboxRotationService();