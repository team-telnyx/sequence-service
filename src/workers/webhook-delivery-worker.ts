import { Worker, Job } from 'bullmq';
import { PrismaClient, WebhookDeliveryStatus } from '@prisma/client';
import { webhooksQueue, defaultWorkerOptions, WebhookJob } from '../services/queue';
import * as crypto from 'crypto';

const prisma = new PrismaClient();

// Exponential backoff delays: 2s, 4s, 8s, 16s, 32s
const RETRY_DELAYS = [2000, 4000, 8000, 16000, 32000];
const MAX_RETRIES = 5;

async function deliverWebhook(job: Job<WebhookJob>) {
  const { deliveryId, attempt = 1 } = job.data;
  
  console.log(`Delivering webhook: ${deliveryId} (attempt ${attempt})`);

  try {
    // Get webhook delivery with config
    const delivery = await prisma.webhookDelivery.findUnique({
      where: { id: deliveryId },
      include: {
        config: true,
      },
    });

    if (!delivery) {
      throw new Error(`Webhook delivery not found: ${deliveryId}`);
    }

    if (!delivery.config.active) {
      console.log(`Webhook config is disabled, skipping delivery: ${delivery.configId}`);
      await prisma.webhookDelivery.update({
        where: { id: deliveryId },
        data: {
          status: WebhookDeliveryStatus.EXPIRED,
          response: 'Webhook configuration is disabled',
          lastAttempt: new Date(),
        },
      });
      return;
    }

    // Update attempt counter
    await prisma.webhookDelivery.update({
      where: { id: deliveryId },
      data: {
        attempts: attempt,
        lastAttempt: new Date(),
      },
    });

    // Prepare payload
    const payloadString = JSON.stringify(delivery.payload);
    const signature = generateHmacSignature(payloadString, delivery.config.secret);

    // Make HTTP request
    const response = await fetch(delivery.config.url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Webhook-Signature': signature,
        'X-Webhook-Timestamp': new Date().getTime().toString(),
        'User-Agent': 'Sequence-Service-Webhook/1.0',
      },
      body: payloadString,
      // 30 second timeout
      signal: AbortSignal.timeout(30000),
    });

    const responseText = await response.text();

    if (response.ok) {
      // Success
      await prisma.webhookDelivery.update({
        where: { id: deliveryId },
        data: {
          status: WebhookDeliveryStatus.DELIVERED,
          response: `${response.status}: ${responseText.substring(0, 1000)}`,
        },
      });

      console.log(`Webhook delivered successfully: ${deliveryId}`);
    } else {
      // HTTP error
      throw new Error(`HTTP ${response.status}: ${responseText}`);
    }
  } catch (error) {
    console.error(`Webhook delivery failed (attempt ${attempt}): ${deliveryId}`, error.message);
    
    await handleWebhookError(deliveryId, attempt, error.message);
  }
}

async function handleWebhookError(deliveryId: string, attempt: number, errorMessage: string) {
  if (attempt >= MAX_RETRIES) {
    // Max retries reached, mark as failed
    await prisma.webhookDelivery.update({
      where: { id: deliveryId },
      data: {
        status: WebhookDeliveryStatus.FAILED,
        response: `Final failure after ${MAX_RETRIES} attempts: ${errorMessage}`,
      },
    });

    console.log(`Webhook delivery failed permanently: ${deliveryId}`);
  } else {
    // Schedule retry with exponential backoff
    const delay = RETRY_DELAYS[attempt - 1] || RETRY_DELAYS[RETRY_DELAYS.length - 1];
    const nextAttempt = new Date(Date.now() + delay);

    await prisma.webhookDelivery.update({
      where: { id: deliveryId },
      data: {
        nextAttempt,
        response: `Retry ${attempt}/${MAX_RETRIES} failed: ${errorMessage}`,
      },
    });

    // Queue retry
    await webhooksQueue.add(
      'deliver-webhook',
      {
        deliveryId,
        attempt: attempt + 1,
      },
      {
        delay,
        jobId: `webhook-${deliveryId}-${attempt + 1}`,
      }
    );

    console.log(`Webhook retry scheduled: ${deliveryId} in ${delay}ms`);
  }
}

function generateHmacSignature(payload: string, secret: string): string {
  const hmac = crypto.createHmac('sha256', secret);
  hmac.update(payload);
  return 'sha256=' + hmac.digest('hex');
}

// Function to verify webhook signature (for testing)
export function verifyWebhookSignature(payload: string, signature: string, secret: string): boolean {
  const expectedSignature = generateHmacSignature(payload, secret);
  return crypto.timingSafeEqual(
    Buffer.from(signature, 'utf8'),
    Buffer.from(expectedSignature, 'utf8')
  );
}

export const webhookDeliveryWorker = new Worker(
  'webhooks',
  deliverWebhook,
  {
    ...defaultWorkerOptions,
    settings: {
      backoffStrategy: (attemptsMade: number) => {
        // Custom backoff is handled in our code, return 0 to disable built-in backoff
        return 0;
      },
    },
  }
);

webhookDeliveryWorker.on('completed', (job) => {
  console.log(`Webhook delivery job completed: ${job.id}`);
});

webhookDeliveryWorker.on('failed', (job, err) => {
  console.error(`Webhook delivery job failed: ${job?.id}`, err.message);
});

webhookDeliveryWorker.on('error', (err) => {
  console.error('Webhook delivery worker error:', err);
});

// Helper function to queue webhook deliveries
export async function queueWebhookDelivery(webhookConfigId: string, payload: any): Promise<void> {
  try {
    // Create webhook delivery record
    const delivery = await prisma.webhookDelivery.create({
      data: {
        configId: webhookConfigId,
        payload,
        status: WebhookDeliveryStatus.PENDING,
      },
    });

    // Queue for immediate delivery
    await webhooksQueue.add(
      'deliver-webhook',
      {
        deliveryId: delivery.id,
        attempt: 1,
      },
      {
        jobId: `webhook-${delivery.id}-1`,
      }
    );

    console.log(`Queued webhook delivery: ${delivery.id}`);
  } catch (error) {
    console.error('Failed to queue webhook delivery:', error);
    throw error;
  }
}

// Function to clean up old webhook deliveries
export async function cleanupOldWebhookDeliveries(daysOld = 30): Promise<number> {
  const cutoffDate = new Date(Date.now() - (daysOld * 24 * 60 * 60 * 1000));
  
  const result = await prisma.webhookDelivery.deleteMany({
    where: {
      createdAt: {
        lt: cutoffDate,
      },
      status: {
        in: [WebhookDeliveryStatus.DELIVERED, WebhookDeliveryStatus.FAILED, WebhookDeliveryStatus.EXPIRED],
      },
    },
  });

  console.log(`Cleaned up ${result.count} old webhook deliveries`);
  return result.count;
}