import Fastify from 'fastify';
import cors from '@fastify/cors';
import helmet from '@fastify/helmet';
import { PrismaClient } from '@prisma/client';
import Redis from 'ioredis';
import { authMiddleware } from './middleware/auth';
import { sequenceRoutes } from './controllers/sequence';
import { mailboxRoutes } from './controllers/mailbox';
import { enrollmentRoutes } from './controllers/enrollment';
import { webhookRoutes } from './controllers/webhook';
import { signalRoutes } from './controllers/signal';
import { startWorkers } from './workers';

// Types
declare module 'fastify' {
  interface FastifyRequest {
    tenant?: {
      id: string;
      name: string;
      apiKey: string;
    };
  }
}

const prisma = new PrismaClient();
const redis = new Redis(process.env.REDIS_URL || 'redis://localhost:6379');

async function buildApp() {
  const fastify = Fastify({
    logger: {
      level: process.env.LOG_LEVEL || 'info',
    },
  });

  // Register plugins
  await fastify.register(cors, {
    origin: true,
  });

  await fastify.register(helmet);

  // Add Prisma and Redis to fastify instance
  fastify.decorate('prisma', prisma);
  fastify.decorate('redis', redis);

  // Register auth middleware
  fastify.addHook('preHandler', authMiddleware);

  // Health check
  fastify.get('/health', async () => {
    return { status: 'ok', timestamp: new Date().toISOString() };
  });

  // Register routes
  await fastify.register(sequenceRoutes, { prefix: '/api/sequences' });
  await fastify.register(mailboxRoutes, { prefix: '/api/mailboxes' });
  await fastify.register(enrollmentRoutes, { prefix: '/api/enrollments' });
  await fastify.register(webhookRoutes, { prefix: '/api/webhooks' });
  await fastify.register(signalRoutes, { prefix: '/api/signals' });

  return fastify;
}

async function start() {
  try {
    const fastify = await buildApp();
    
    // Start background workers
    await startWorkers();
    
    const port = parseInt(process.env.PORT || '3000');
    await fastify.listen({ port, host: '0.0.0.0' });
    
    fastify.log.info(`Server listening on port ${port}`);
  } catch (err) {
    console.error('Error starting server:', err);
    process.exit(1);
  }
}

// Graceful shutdown
process.on('SIGINT', async () => {
  try {
    await prisma.$disconnect();
    await redis.disconnect();
    process.exit(0);
  } catch (err) {
    console.error('Error during shutdown:', err);
    process.exit(1);
  }
});

if (require.main === module) {
  start();
}

export { buildApp };