import { Queue, Worker, QueueOptions, WorkerOptions } from 'bullmq';
import Redis from 'ioredis';

const redis = new Redis(process.env.REDIS_URL || 'redis://localhost:6379');

// Queue configurations
const defaultQueueOptions: QueueOptions = {
  connection: redis,
  defaultJobOptions: {
    attempts: 3,
    backoff: {
      type: 'exponential',
      delay: 5000,
    },
    removeOnComplete: 100,
    removeOnFail: 50,
  },
};

const defaultWorkerOptions: WorkerOptions = {
  connection: redis,
  concurrency: parseInt(process.env.WORKER_CONCURRENCY || '10'),
};

// Queue definitions
export const sequenceStepsQueue = new Queue('sequence-steps', {
  ...defaultQueueOptions,
  defaultJobOptions: {
    ...defaultQueueOptions.defaultJobOptions,
    delay: 0, // Steps are scheduled with specific delays
  },
});

export const warmupQueue = new Queue('warmup', defaultQueueOptions);

export const watchdogQueue = new Queue('watchdog', {
  ...defaultQueueOptions,
  defaultJobOptions: {
    ...defaultQueueOptions.defaultJobOptions,
    repeat: { pattern: '*/5 * * * *' }, // Every 5 minutes
  },
});

export const webhooksQueue = new Queue('webhooks', {
  ...defaultQueueOptions,
  defaultJobOptions: {
    ...defaultQueueOptions.defaultJobOptions,
    attempts: 5,
    backoff: {
      type: 'exponential',
      delay: 2000,
    },
  },
});

// Job data interfaces
export interface SequenceStepJob {
  enrollmentStepId: string;
  tenantId: string;
  scheduledAt: Date;
}

export interface WarmupJob {
  mailboxId: string;
  tenantId: string;
  targetCount: number;
}

export interface WatchdogJob {
  type: 'signal-detection' | 'oof-resume' | 'circuit-breaker' | 'sent-today-reset';
  tenantId?: string;
  mailboxId?: string;
}

export interface WebhookJob {
  deliveryId: string;
  attempt: number;
}

// Helper functions
export function createSequenceStepJob(data: SequenceStepJob, delay?: number) {
  return sequenceStepsQueue.add('process-step', data, {
    delay: delay || 0,
    jobId: `step-${data.enrollmentStepId}`,
  });
}

export function createWarmupJob(data: WarmupJob) {
  return warmupQueue.add('warmup-mailbox', data, {
    jobId: `warmup-${data.mailboxId}`,
  });
}

export function createWatchdogJob(data: WatchdogJob) {
  const jobId = data.mailboxId 
    ? `watchdog-${data.type}-${data.mailboxId}`
    : `watchdog-${data.type}-global`;
    
  return watchdogQueue.add('watchdog-check', data, {
    jobId,
  });
}

export function createWebhookJob(data: WebhookJob, delay?: number) {
  return webhooksQueue.add('deliver-webhook', data, {
    delay: delay || 0,
    jobId: `webhook-${data.deliveryId}-${data.attempt}`,
  });
}

// Queue cleanup on shutdown
export function cleanupQueues() {
  return Promise.all([
    sequenceStepsQueue.close(),
    warmupQueue.close(),
    watchdogQueue.close(),
    webhooksQueue.close(),
  ]);
}

export { defaultWorkerOptions };