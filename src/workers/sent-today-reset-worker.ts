import { Worker, Job } from 'bullmq';
import { watchdogQueue, defaultWorkerOptions, WatchdogJob } from '../services/queue';
import { mailboxRotationService } from '../services/mailbox-rotation';

async function resetSentTodayCounters(job: Job<WatchdogJob>) {
  if (job.data.type !== 'sent-today-reset') {
    return;
  }

  console.log('Starting sentToday counter reset...');

  try {
    let totalReset: number;

    if (job.data.tenantId) {
      // Reset for specific tenant
      totalReset = await mailboxRotationService.resetDailySendCounters(job.data.tenantId);
      console.log(`Reset sentToday counters for ${totalReset} mailboxes in tenant ${job.data.tenantId}`);
    } else {
      // Reset for all tenants
      totalReset = await mailboxRotationService.resetDailySendCounters();
      console.log(`Reset sentToday counters for ${totalReset} mailboxes across all tenants`);
    }

    // Create webhook notification if configured
    if (totalReset > 0) {
      await createSentTodayResetWebhook(job.data.tenantId, totalReset);
    }

    console.log('sentToday counter reset completed');
  } catch (error) {
    console.error('sentToday reset worker error:', error);
    throw error;
  }
}

async function createSentTodayResetWebhook(tenantId: string | undefined, mailboxCount: number) {
  try {
    // This is a system-level event, but we can still notify tenants
    const { PrismaClient } = await import('@prisma/client');
    const prisma = new PrismaClient();

    // If specific tenant, only notify them
    const tenantWhere = tenantId ? { tenantId } : {};

    const webhookConfigs = await prisma.webhookConfig.findMany({
      where: {
        ...tenantWhere,
        active: true,
        events: {
          has: 'mailbox.daily_reset',
        },
      },
    });

    if (webhookConfigs.length === 0) {
      return;
    }

    const payload = {
      event: 'mailbox.daily_reset',
      timestamp: new Date().toISOString(),
      data: {
        mailboxes_reset: mailboxCount,
        reset_time: 'midnight_utc',
      },
    };

    // Create webhook delivery records
    for (const config of webhookConfigs) {
      await prisma.webhookDelivery.create({
        data: {
          configId: config.id,
          payload: {
            ...payload,
            tenant_id: config.tenantId,
          },
          status: 'PENDING',
        },
      });
    }

    console.log(`Created ${webhookConfigs.length} daily reset webhook deliveries`);
    await prisma.$disconnect();
  } catch (error) {
    console.error('Failed to create daily reset webhook:', error);
  }
}

export const sentTodayResetWorker = new Worker(
  'watchdog',
  async (job: Job<WatchdogJob>) => {
    if (job.data.type === 'sent-today-reset') {
      await resetSentTodayCounters(job);
    }
  },
  defaultWorkerOptions
);

// Schedule sentToday reset daily at midnight UTC
watchdogQueue.add(
  'watchdog-check',
  { type: 'sent-today-reset' },
  {
    repeat: { pattern: '0 0 * * *' }, // Midnight UTC daily
    jobId: 'sent-today-reset-cron',
  }
);

sentTodayResetWorker.on('completed', (job) => {
  if (job.data.type === 'sent-today-reset') {
    console.log('sentToday reset job completed');
  }
});

sentTodayResetWorker.on('failed', (job, err) => {
  if (job?.data.type === 'sent-today-reset') {
    console.error('sentToday reset job failed:', err.message);
  }
});

sentTodayResetWorker.on('error', (err) => {
  console.error('sentToday reset worker error:', err);
});