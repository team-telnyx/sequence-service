import { Worker, Job } from 'bullmq';
import { PrismaClient, MailboxStatus, SignalType } from '@prisma/client';
import { watchdogQueue, defaultWorkerOptions, WatchdogJob } from '../services/queue';

const prisma = new PrismaClient();

async function detectSignals(job: Job<WatchdogJob>) {
  if (job.data.type !== 'signal-detection') {
    return;
  }

  console.log('Starting signal detection sweep...');

  try {
    // Get all active mailboxes (optionally filtered by tenant/mailbox)
    const where: any = {
      status: MailboxStatus.ACTIVE,
    };

    if (job.data.tenantId) {
      where.tenantId = job.data.tenantId;
    }

    if (job.data.mailboxId) {
      where.id = job.data.mailboxId;
    }

    const mailboxes = await prisma.mailbox.findMany({
      where,
      select: {
        id: true,
        email: true,
        tenantId: true,
        refreshToken: true,
        accessToken: true,
        tokenExpiresAt: true,
      },
    });

    console.log(`Checking signals for ${mailboxes.length} mailboxes`);

    for (const mailbox of mailboxes) {
      try {
        await detectSignalsForMailbox(mailbox);
      } catch (error) {
        console.error(`Failed to detect signals for mailbox ${mailbox.email}:`, error);
        // Continue with other mailboxes
      }
    }

    console.log('Signal detection sweep completed');
  } catch (error) {
    console.error('Signal detection worker error:', error);
    throw error;
  }
}

async function detectSignalsForMailbox(mailbox: any) {
  if (process.env.GMAIL_ENABLED !== 'true') {
    // In stub mode, simulate signal detection
    console.log(`[STUB] Checking signals for mailbox: ${mailbox.email}`);
    
    // Randomly generate some test signals for demo purposes
    if (Math.random() < 0.1) { // 10% chance
      const signalTypes: SignalType[] = ['REPLY', 'BOUNCE', 'OUT_OF_OFFICE'];
      const randomType = signalTypes[Math.floor(Math.random() * signalTypes.length)];
      
      console.log(`[STUB] Detected ${randomType} signal for ${mailbox.email}`);
      
      // Create signal record
      await prisma.signal.create({
        data: {
          type: randomType,
          mailboxId: mailbox.id,
          data: {
            source: 'stub',
            timestamp: new Date().toISOString(),
          },
        },
      });

      // Process signal effects
      await processSignalEffects(randomType, mailbox);
    }
    
    return;
  }

  // TODO: Implement actual Gmail API integration
  console.log(`Checking Gmail for signals: ${mailbox.email}`);
  
  try {
    // This would use the Gmail API to:
    // 1. Check for new replies to sent emails
    // 2. Check for bounces
    // 3. Check for out-of-office responses
    // 4. Process click/open tracking (if implemented)
    
    throw new Error('Gmail integration not implemented yet');
  } catch (error) {
    console.error(`Gmail signal detection failed for ${mailbox.email}:`, error);
  }
}

async function processSignalEffects(signalType: SignalType, mailbox: any) {
  switch (signalType) {
    case 'REPLY':
      // Pause all active enrollments for this contact
      // This would require knowing which contact replied
      console.log(`[STUB] Processing REPLY signal effects for ${mailbox.email}`);
      break;
      
    case 'BOUNCE':
      // Increment bounce counter, possibly pause mailbox
      console.log(`[STUB] Processing BOUNCE signal effects for ${mailbox.email}`);
      break;
      
    case 'OUT_OF_OFFICE':
      // Mark mailbox as out of office, pause sequences
      console.log(`[STUB] Processing OUT_OF_OFFICE signal effects for ${mailbox.email}`);
      await prisma.mailbox.update({
        where: { id: mailbox.id },
        data: { status: MailboxStatus.OUT_OF_OFFICE },
      });
      break;
      
    case 'UNSUBSCRIBE':
      // Mark contact as unsubscribed, pause all their enrollments
      console.log(`[STUB] Processing UNSUBSCRIBE signal effects for ${mailbox.email}`);
      break;
      
    default:
      console.log(`[STUB] Processing ${signalType} signal effects for ${mailbox.email}`);
  }
}

export const signalDetectionWorker = new Worker(
  'watchdog',
  async (job: Job<WatchdogJob>) => {
    if (job.data.type === 'signal-detection') {
      await detectSignals(job);
    }
  },
  defaultWorkerOptions
);

// Schedule signal detection every 5 minutes
watchdogQueue.add(
  'watchdog-check',
  { type: 'signal-detection' },
  {
    repeat: { pattern: '*/5 * * * *' }, // Every 5 minutes
    jobId: 'signal-detection-cron',
  }
);

signalDetectionWorker.on('completed', (job) => {
  if (job.data.type === 'signal-detection') {
    console.log('Signal detection job completed');
  }
});

signalDetectionWorker.on('failed', (job, err) => {
  if (job?.data.type === 'signal-detection') {
    console.error('Signal detection job failed:', err.message);
  }
});

signalDetectionWorker.on('error', (err) => {
  console.error('Signal detection worker error:', err);
});