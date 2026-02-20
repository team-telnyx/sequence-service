import { Worker, Job } from 'bullmq';
import { PrismaClient } from '@prisma/client';
import { watchdogQueue, defaultWorkerOptions, WatchdogJob } from '../services/queue';
import { mailboxRotationService } from '../services/mailbox-rotation';

const prisma = new PrismaClient();

async function checkCircuitBreaker(job: Job<WatchdogJob>) {
  if (job.data.type !== 'circuit-breaker') {
    return;
  }

  console.log('Starting circuit breaker check...');

  try {
    // Get all tenants to check (or specific tenant if provided)
    let tenantsToCheck: string[];
    
    if (job.data.tenantId) {
      tenantsToCheck = [job.data.tenantId];
    } else {
      const tenants = await prisma.tenant.findMany({
        select: { id: true },
      });
      tenantsToCheck = tenants.map(t => t.id);
    }

    console.log(`Checking circuit breaker for ${tenantsToCheck.length} tenants`);

    let triggeredCount = 0;

    for (const tenantId of tenantsToCheck) {
      try {
        const result = await mailboxRotationService.checkCircuitBreaker(tenantId, 24); // 24-hour window
        
        console.log(`Tenant ${tenantId} - Bounce rate: ${result.bounceRate}% (${result.bounces}/${result.totalEmails})`);
        
        if (result.shouldTrigger) {
          console.log(`🚨 Circuit breaker triggered for tenant ${tenantId}`);
          
          const pausedMailboxes = await mailboxRotationService.triggerCircuitBreaker(
            tenantId,
            `High bounce rate detected: ${result.bounceRate}% (${result.bounces}/${result.totalEmails} emails)`
          );
          
          console.log(`Paused ${pausedMailboxes} mailboxes for tenant ${tenantId}`);
          
          // Create webhook notification
          await createCircuitBreakerWebhook(tenantId, result);
          
          triggeredCount++;
        }
      } catch (error) {
        console.error(`Failed to check circuit breaker for tenant ${tenantId}:`, error);
      }
    }

    console.log(`Circuit breaker check completed. Triggered for ${triggeredCount} tenants.`);
  } catch (error) {
    console.error('Circuit breaker worker error:', error);
    throw error;
  }
}

async function createCircuitBreakerWebhook(tenantId: string, circuitBreakerResult: any) {
  try {
    // Get active webhook configurations for this tenant
    const webhookConfigs = await prisma.webhookConfig.findMany({
      where: {
        tenantId,
        active: true,
        events: {
          has: 'circuit_breaker.triggered',
        },
      },
    });

    if (webhookConfigs.length === 0) {
      console.log(`No webhook configs for circuit breaker events for tenant ${tenantId}`);
      return;
    }

    const payload = {
      event: 'circuit_breaker.triggered',
      timestamp: new Date().toISOString(),
      tenant_id: tenantId,
      data: {
        bounce_rate: circuitBreakerResult.bounceRate,
        total_emails: circuitBreakerResult.totalEmails,
        bounces: circuitBreakerResult.bounces,
        time_window: '24h',
        action: 'mailboxes_paused',
      },
    };

    // Create webhook delivery records
    for (const config of webhookConfigs) {
      await prisma.webhookDelivery.create({
        data: {
          configId: config.id,
          payload,
          status: 'PENDING',
        },
      });
    }

    console.log(`Created ${webhookConfigs.length} circuit breaker webhook deliveries for tenant ${tenantId}`);
  } catch (error) {
    console.error(`Failed to create circuit breaker webhook for tenant ${tenantId}:`, error);
  }
}

export const circuitBreakerWorker = new Worker(
  'watchdog',
  async (job: Job<WatchdogJob>) => {
    if (job.data.type === 'circuit-breaker') {
      await checkCircuitBreaker(job);
    }
  },
  defaultWorkerOptions
);

// Schedule circuit breaker check every hour
watchdogQueue.add(
  'watchdog-check',
  { type: 'circuit-breaker' },
  {
    repeat: { pattern: '0 * * * *' }, // Every hour
    jobId: 'circuit-breaker-cron',
  }
);

circuitBreakerWorker.on('completed', (job) => {
  if (job.data.type === 'circuit-breaker') {
    console.log('Circuit breaker job completed');
  }
});

circuitBreakerWorker.on('failed', (job, err) => {
  if (job?.data.type === 'circuit-breaker') {
    console.error('Circuit breaker job failed:', err.message);
  }
});

circuitBreakerWorker.on('error', (err) => {
  console.error('Circuit breaker worker error:', err);
});