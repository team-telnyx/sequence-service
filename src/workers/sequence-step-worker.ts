import { Worker, Job } from 'bullmq';
import { PrismaClient, SequenceEnrollmentStepStatus } from '@prisma/client';
import { sequenceStepsQueue, defaultWorkerOptions, SequenceStepJob } from '../services/queue';
import { mailboxRotationService } from '../services/mailbox-rotation';
import { templateService } from '../services/template';

const prisma = new PrismaClient();

// Retry delays: 30s, 90s, 270s
const RETRY_DELAYS = [30000, 90000, 270000];

async function processSequenceStep(job: Job<SequenceStepJob>) {
  const { enrollmentStepId, tenantId } = job.data;
  
  console.log(`Processing sequence step: ${enrollmentStepId}`);

  try {
    // Get enrollment step with all related data
    const enrollmentStep = await prisma.sequenceEnrollmentStep.findFirst({
      where: {
        id: enrollmentStepId,
        enrollment: {
          sequence: {
            tenantId,
          },
        },
      },
      include: {
        enrollment: {
          include: {
            sequence: true,
          },
        },
        step: true,
        mailbox: {
          select: {
            id: true,
            email: true,
            displayName: true,
            status: true,
          },
        },
      },
    });

    if (!enrollmentStep) {
      throw new Error(`Enrollment step not found: ${enrollmentStepId}`);
    }

    // Check if enrollment is still active
    if (enrollmentStep.enrollment.status !== 'ACTIVE') {
      console.log(`Skipping step - enrollment not active: ${enrollmentStep.enrollment.status}`);
      return;
    }

    // Check if sequence is still active
    if (enrollmentStep.enrollment.sequence.status !== 'ACTIVE') {
      console.log(`Skipping step - sequence not active: ${enrollmentStep.enrollment.sequence.status}`);
      return;
    }

    // Check if step is still pending
    if (enrollmentStep.status !== SequenceEnrollmentStepStatus.PENDING) {
      console.log(`Skipping step - not pending: ${enrollmentStep.status}`);
      return;
    }

    // Select a mailbox if one isn't already assigned
    let mailbox = enrollmentStep.mailbox;
    if (!mailbox) {
      const selectedMailbox = await mailboxRotationService.selectMailbox({
        tenantId,
        requireMinimumSends: 1,
      });

      if (!selectedMailbox) {
        throw new Error('No available mailboxes for sending');
      }

      mailbox = selectedMailbox;
      
      // Update enrollment step with selected mailbox
      await prisma.sequenceEnrollmentStep.update({
        where: { id: enrollmentStepId },
        data: { mailboxId: selectedMailbox.id },
      });
    }

    // Reserve send slot
    const slotReserved = await mailboxRotationService.reserveSend(mailbox.id);
    if (!slotReserved) {
      throw new Error('Failed to reserve send slot - mailbox at capacity');
    }

    try {
      // Render email content
      const context = templateService.createContext(
        enrollmentStep.enrollment,
        enrollmentStep.step,
        mailbox
      );

      const renderedSubject = templateService.renderSubject(
        enrollmentStep.step.subject,
        context
      );

      const renderedBody = templateService.renderBody(
        enrollmentStep.step.body,
        context
      );

      // In production, this would send the actual email
      // For now, we'll simulate by creating a SentEmail record
      if (process.env.GMAIL_ENABLED === 'true') {
        // TODO: Implement actual Gmail sending
        throw new Error('Gmail sending not implemented yet');
      } else {
        // Stub mode - simulate sending
        console.log(`[STUB] Sending email from ${mailbox.email} to ${enrollmentStep.enrollment.contactEmail}`);
        console.log(`[STUB] Subject: ${renderedSubject}`);
        console.log(`[STUB] Body preview: ${renderedBody.substring(0, 100)}...`);

        // Create SentEmail record
        const sentEmail = await prisma.sentEmail.create({
          data: {
            messageId: `stub-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
            subject: renderedSubject,
            body: renderedBody,
            toEmail: enrollmentStep.enrollment.contactEmail,
            toName: enrollmentStep.enrollment.contactName,
            fromEmail: mailbox.email,
            fromName: mailbox.displayName,
            mailboxId: mailbox.id,
            enrollmentStepId: enrollmentStep.id,
          },
        });

        // Update enrollment step status
        await prisma.sequenceEnrollmentStep.update({
          where: { id: enrollmentStepId },
          data: {
            status: SequenceEnrollmentStepStatus.SENT,
            sentAt: new Date(),
          },
        });

        // Schedule next step if this isn't the last one
        const nextStep = await prisma.sequenceStep.findFirst({
          where: {
            sequenceId: enrollmentStep.enrollment.sequenceId,
            stepNumber: enrollmentStep.step.stepNumber + 1,
          },
        });

        if (nextStep) {
          const nextEnrollmentStep = await prisma.sequenceEnrollmentStep.findFirst({
            where: {
              enrollmentId: enrollmentStep.enrollment.id,
              stepId: nextStep.id,
            },
          });

          if (nextEnrollmentStep) {
            const nextScheduledTime = new Date(Date.now() + (nextStep.delayHours * 60 * 60 * 1000));
            
            await prisma.sequenceEnrollmentStep.update({
              where: { id: nextEnrollmentStep.id },
              data: { scheduledAt: nextScheduledTime },
            });

            // Queue next step
            await sequenceStepsQueue.add(
              'process-step',
              {
                enrollmentStepId: nextEnrollmentStep.id,
                tenantId,
                scheduledAt: nextScheduledTime,
              },
              {
                delay: nextStep.delayHours * 60 * 60 * 1000,
                jobId: `step-${nextEnrollmentStep.id}`,
              }
            );
          }
        }

        console.log(`Successfully processed sequence step: ${enrollmentStepId}`);
      }
    } catch (sendError) {
      // Release the reserved send slot
      await mailboxRotationService.releaseSend(mailbox.id);
      throw sendError;
    }
  } catch (error) {
    console.error(`Failed to process sequence step ${enrollmentStepId}:`, error);
    
    // Handle retries with exponential backoff
    const attemptNumber = job.attemptsMade;
    if (attemptNumber < RETRY_DELAYS.length) {
      const delay = RETRY_DELAYS[attemptNumber];
      console.log(`Scheduling retry ${attemptNumber + 1} in ${delay}ms`);
      
      throw new Error(`Step processing failed, will retry: ${error.message}`);
    } else {
      // Mark as failed after all retries exhausted
      await prisma.sequenceEnrollmentStep.updateMany({
        where: { id: enrollmentStepId },
        data: { status: SequenceEnrollmentStepStatus.BOUNCED },
      });
      
      throw new Error(`Step processing failed permanently: ${error.message}`);
    }
  }
}

export const sequenceStepWorker = new Worker(
  'sequence-steps',
  processSequenceStep,
  {
    ...defaultWorkerOptions,
    settings: {
      backoffStrategy: (attemptsMade: number) => {
        if (attemptsMade <= RETRY_DELAYS.length) {
          return RETRY_DELAYS[attemptsMade - 1];
        }
        return 0; // No more retries
      },
    },
  }
);

sequenceStepWorker.on('completed', (job) => {
  console.log(`Sequence step job completed: ${job.id}`);
});

sequenceStepWorker.on('failed', (job, err) => {
  console.error(`Sequence step job failed: ${job?.id}`, err.message);
});

sequenceStepWorker.on('error', (err) => {
  console.error('Sequence step worker error:', err);
});