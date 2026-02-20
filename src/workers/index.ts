import { sequenceStepWorker } from './sequence-step-worker';
import { signalDetectionWorker } from './signal-detection-worker';
import { oofResumeWorker } from './oof-resume-worker';
import { circuitBreakerWorker } from './circuit-breaker-worker';
import { sentTodayResetWorker } from './sent-today-reset-worker';
import { webhookDeliveryWorker } from './webhook-delivery-worker';

export async function startWorkers() {
  console.log('Starting background workers...');

  try {
    // Start all workers
    await Promise.all([
      sequenceStepWorker.run(),
      signalDetectionWorker.run(),
      oofResumeWorker.run(),
      circuitBreakerWorker.run(),
      sentTodayResetWorker.run(),
      webhookDeliveryWorker.run(),
    ]);

    console.log('All workers started successfully');
  } catch (error) {
    console.error('Failed to start workers:', error);
    throw error;
  }
}

export async function stopWorkers() {
  console.log('Stopping background workers...');

  try {
    await Promise.all([
      sequenceStepWorker.close(),
      signalDetectionWorker.close(),
      oofResumeWorker.close(),
      circuitBreakerWorker.close(),
      sentTodayResetWorker.close(),
      webhookDeliveryWorker.close(),
    ]);

    console.log('All workers stopped successfully');
  } catch (error) {
    console.error('Error stopping workers:', error);
    throw error;
  }
}

// Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('Received SIGTERM, shutting down workers...');
  await stopWorkers();
  process.exit(0);
});

process.on('SIGINT', async () => {
  console.log('Received SIGINT, shutting down workers...');
  await stopWorkers();
  process.exit(0);
});