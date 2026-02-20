import { Worker, Job } from 'bullmq';
import { PrismaClient, MailboxStatus } from '@prisma/client';
import { watchdogQueue, defaultWorkerOptions, WatchdogJob } from '../services/queue';

const prisma = new PrismaClient();

async function resumeFromOutOfOffice(job: Job<WatchdogJob>) {
  if (job.data.type !== 'oof-resume') {
    return;
  }

  console.log('Starting out-of-office resume check...');

  try {
    // Get all mailboxes marked as out of office
    const where: any = {
      status: MailboxStatus.OUT_OF_OFFICE,
    };

    if (job.data.tenantId) {
      where.tenantId = job.data.tenantId;
    }

    if (job.data.mailboxId) {
      where.id = job.data.mailboxId;
    }

    const oofMailboxes = await prisma.mailbox.findMany({
      where,
      select: {
        id: true,
        email: true,
        tenantId: true,
        updatedAt: true,
      },
    });

    console.log(`Checking ${oofMailboxes.length} out-of-office mailboxes for resume`);

    let resumedCount = 0;

    for (const mailbox of oofMailboxes) {
      try {
        const shouldResume = await checkIfShouldResume(mailbox);
        
        if (shouldResume) {
          await resumeMailbox(mailbox);
          resumedCount++;
        }
      } catch (error) {
        console.error(`Failed to check resume status for mailbox ${mailbox.email}:`, error);
      }
    }

    console.log(`Out-of-office resume check completed. Resumed ${resumedCount} mailboxes.`);
  } catch (error) {
    console.error('OOF resume worker error:', error);
    throw error;
  }
}

async function checkIfShouldResume(mailbox: any): Promise<boolean> {
  if (process.env.GMAIL_ENABLED !== 'true') {
    // In stub mode, simulate OOF resume logic
    console.log(`[STUB] Checking if ${mailbox.email} should resume from OOF`);
    
    // Simple logic: resume if mailbox has been OOF for more than 1 day
    const oneDayAgo = new Date(Date.now() - (24 * 60 * 60 * 1000));
    const shouldResume = mailbox.updatedAt < oneDayAgo;
    
    if (shouldResume) {
      console.log(`[STUB] Mailbox ${mailbox.email} should resume - OOF for > 1 day`);
    }
    
    return shouldResume;
  }

  // TODO: Implement actual Gmail API integration
  try {
    // This would use the Gmail API to:
    // 1. Check if auto-reply is still active
    // 2. Look for recent out-of-office signals
    // 3. Check if the mailbox owner has sent any emails recently
    
    console.log(`Checking Gmail OOF status for: ${mailbox.email}`);
    throw new Error('Gmail OOF detection not implemented yet');
  } catch (error) {
    console.error(`Gmail OOF check failed for ${mailbox.email}:`, error);
    return false;
  }
}

async function resumeMailbox(mailbox: any) {
  console.log(`Resuming mailbox from out-of-office: ${mailbox.email}`);

  try {
    // Update mailbox status to ACTIVE
    await prisma.mailbox.update({
      where: { id: mailbox.id },
      data: { 
        status: MailboxStatus.ACTIVE,
        updatedAt: new Date(),
      },
    });

    // Reactivate paused enrollments that were paused due to OOF
    // Note: This is a simplified approach. In practice, you might want to track
    // which enrollments were paused specifically due to OOF vs other reasons
    const pausedEnrollments = await prisma.sequenceEnrollment.findMany({
      where: {
        status: 'PAUSED',
        sequence: {
          tenantId: mailbox.tenantId,
        },
        // Additional logic could be added here to identify OOF-specific pauses
      },
    });

    if (pausedEnrollments.length > 0) {
      await prisma.sequenceEnrollment.updateMany({
        where: {
          id: {
            in: pausedEnrollments.map(e => e.id),
          },
        },
        data: {
          status: 'ACTIVE',
        },
      });

      console.log(`Reactivated ${pausedEnrollments.length} enrollments for ${mailbox.email}`);
    }

    // TODO: Queue up any pending sequence steps that were delayed due to OOF
    // This might involve rescheduling steps that were supposed to be sent while OOF

    console.log(`Successfully resumed mailbox: ${mailbox.email}`);
  } catch (error) {
    console.error(`Failed to resume mailbox ${mailbox.email}:`, error);
    throw error;
  }
}

export const oofResumeWorker = new Worker(
  'watchdog',
  async (job: Job<WatchdogJob>) => {
    if (job.data.type === 'oof-resume') {
      await resumeFromOutOfOffice(job);
    }
  },
  defaultWorkerOptions
);

// Schedule OOF resume check daily at 9 AM UTC
watchdogQueue.add(
  'watchdog-check',
  { type: 'oof-resume' },
  {
    repeat: { pattern: '0 9 * * *' }, // 9 AM UTC daily
    jobId: 'oof-resume-cron',
  }
);

oofResumeWorker.on('completed', (job) => {
  if (job.data.type === 'oof-resume') {
    console.log('OOF resume job completed');
  }
});

oofResumeWorker.on('failed', (job, err) => {
  if (job?.data.type === 'oof-resume') {
    console.error('OOF resume job failed:', err.message);
  }
});

oofResumeWorker.on('error', (err) => {
  console.error('OOF resume worker error:', err);
});