import { FastifyRequest, FastifyReply } from 'fastify';
import { PrismaClient } from '@prisma/client';

const prisma = new PrismaClient();

export async function authMiddleware(
  request: FastifyRequest,
  reply: FastifyReply
) {
  // Skip auth for health check
  if (request.url === '/health') {
    return;
  }

  const apiKey = request.headers['x-api-key'] as string;

  if (!apiKey) {
    return reply.status(401).send({
      error: 'Missing X-API-Key header',
      code: 'MISSING_API_KEY',
    });
  }

  try {
    const tenant = await prisma.tenant.findUnique({
      where: { apiKey },
      select: {
        id: true,
        name: true,
        apiKey: true,
      },
    });

    if (!tenant) {
      return reply.status(401).send({
        error: 'Invalid API key',
        code: 'INVALID_API_KEY',
      });
    }

    // Add tenant to request context
    request.tenant = tenant;
  } catch (error) {
    request.log.error('Error during authentication:', error);
    return reply.status(500).send({
      error: 'Authentication error',
      code: 'AUTH_ERROR',
    });
  }
}