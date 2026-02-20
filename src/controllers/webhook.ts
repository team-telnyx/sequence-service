import { FastifyPluginAsync, FastifyRequest, FastifyReply } from 'fastify';
import { z } from 'zod';

// Request schemas
const createWebhookSchema = z.object({
  url: z.string().url(),
  secret: z.string().min(1),
  events: z.array(z.string()).min(1),
});

const updateWebhookSchema = z.object({
  url: z.string().url().optional(),
  secret: z.string().min(1).optional(),
  events: z.array(z.string()).min(1).optional(),
  active: z.boolean().optional(),
});

type CreateWebhookRequest = FastifyRequest<{
  Body: z.infer<typeof createWebhookSchema>;
}>;

type UpdateWebhookRequest = FastifyRequest<{
  Params: { id: string };
  Body: z.infer<typeof updateWebhookSchema>;
}>;

type GetWebhookRequest = FastifyRequest<{
  Params: { id: string };
}>;

type ListWebhooksRequest = FastifyRequest<{
  Querystring: {
    active?: string;
    limit?: string;
    offset?: string;
  };
}>;

type WebhookDeliveriesRequest = FastifyRequest<{
  Params: { id: string };
  Querystring: {
    status?: string;
    limit?: string;
    offset?: string;
  };
}>;

export const webhookRoutes: FastifyPluginAsync = async (fastify) => {
  const { prisma } = fastify;

  // GET /api/webhooks - List webhooks
  fastify.get('/', async (request: ListWebhooksRequest, reply: FastifyReply) => {
    try {
      const { active, limit = '50', offset = '0' } = request.query;
      const tenantId = request.tenant!.id;

      const where = {
        tenantId,
        ...(active !== undefined && { active: active === 'true' }),
      };

      const [webhooks, total] = await Promise.all([
        prisma.webhookConfig.findMany({
          where,
          select: {
            id: true,
            url: true,
            events: true,
            active: true,
            createdAt: true,
            updatedAt: true,
            _count: {
              select: {
                deliveries: true,
              },
            },
            // Exclude secret for security
          },
          skip: parseInt(offset),
          take: parseInt(limit),
          orderBy: { createdAt: 'desc' },
        }),
        prisma.webhookConfig.count({ where }),
      ]);

      return {
        data: webhooks,
        meta: {
          total,
          limit: parseInt(limit),
          offset: parseInt(offset),
        },
      };
    } catch (error) {
      request.log.error('Error listing webhooks:', error);
      return reply.status(500).send({
        error: 'Failed to list webhooks',
        code: 'LIST_WEBHOOKS_ERROR',
      });
    }
  });

  // GET /api/webhooks/:id - Get webhook
  fastify.get('/:id', async (request: GetWebhookRequest, reply: FastifyReply) => {
    try {
      const { id } = request.params;
      const tenantId = request.tenant!.id;

      const webhook = await prisma.webhookConfig.findFirst({
        where: { id, tenantId },
        select: {
          id: true,
          url: true,
          events: true,
          active: true,
          createdAt: true,
          updatedAt: true,
          _count: {
            select: {
              deliveries: true,
            },
          },
          // Exclude secret for security
        },
      });

      if (!webhook) {
        return reply.status(404).send({
          error: 'Webhook not found',
          code: 'WEBHOOK_NOT_FOUND',
        });
      }

      return { data: webhook };
    } catch (error) {
      request.log.error('Error getting webhook:', error);
      return reply.status(500).send({
        error: 'Failed to get webhook',
        code: 'GET_WEBHOOK_ERROR',
      });
    }
  });

  // POST /api/webhooks - Create webhook
  fastify.post('/', async (request: CreateWebhookRequest, reply: FastifyReply) => {
    try {
      const body = createWebhookSchema.parse(request.body);
      const tenantId = request.tenant!.id;

      const webhook = await prisma.webhookConfig.create({
        data: {
          url: body.url,
          secret: body.secret,
          events: body.events,
          tenantId,
        },
        select: {
          id: true,
          url: true,
          events: true,
          active: true,
          createdAt: true,
          updatedAt: true,
          // Exclude secret for security
        },
      });

      return reply.status(201).send({ data: webhook });
    } catch (error) {
      if (error instanceof z.ZodError) {
        return reply.status(400).send({
          error: 'Validation error',
          details: error.errors,
          code: 'VALIDATION_ERROR',
        });
      }
      
      request.log.error('Error creating webhook:', error);
      return reply.status(500).send({
        error: 'Failed to create webhook',
        code: 'CREATE_WEBHOOK_ERROR',
      });
    }
  });

  // PUT /api/webhooks/:id - Update webhook
  fastify.put('/:id', async (request: UpdateWebhookRequest, reply: FastifyReply) => {
    try {
      const { id } = request.params;
      const body = updateWebhookSchema.parse(request.body);
      const tenantId = request.tenant!.id;

      const webhook = await prisma.webhookConfig.updateMany({
        where: { id, tenantId },
        data: body,
      });

      if (webhook.count === 0) {
        return reply.status(404).send({
          error: 'Webhook not found',
          code: 'WEBHOOK_NOT_FOUND',
        });
      }

      const updatedWebhook = await prisma.webhookConfig.findFirst({
        where: { id, tenantId },
        select: {
          id: true,
          url: true,
          events: true,
          active: true,
          createdAt: true,
          updatedAt: true,
          // Exclude secret for security
        },
      });

      return { data: updatedWebhook };
    } catch (error) {
      if (error instanceof z.ZodError) {
        return reply.status(400).send({
          error: 'Validation error',
          details: error.errors,
          code: 'VALIDATION_ERROR',
        });
      }
      
      request.log.error('Error updating webhook:', error);
      return reply.status(500).send({
        error: 'Failed to update webhook',
        code: 'UPDATE_WEBHOOK_ERROR',
      });
    }
  });

  // DELETE /api/webhooks/:id - Delete webhook
  fastify.delete('/:id', async (request: GetWebhookRequest, reply: FastifyReply) => {
    try {
      const { id } = request.params;
      const tenantId = request.tenant!.id;

      const webhook = await prisma.webhookConfig.deleteMany({
        where: { id, tenantId },
      });

      if (webhook.count === 0) {
        return reply.status(404).send({
          error: 'Webhook not found',
          code: 'WEBHOOK_NOT_FOUND',
        });
      }

      return reply.status(204).send();
    } catch (error) {
      request.log.error('Error deleting webhook:', error);
      return reply.status(500).send({
        error: 'Failed to delete webhook',
        code: 'DELETE_WEBHOOK_ERROR',
      });
    }
  });

  // GET /api/webhooks/:id/deliveries - List webhook deliveries
  fastify.get('/:id/deliveries', async (request: WebhookDeliveriesRequest, reply: FastifyReply) => {
    try {
      const { id } = request.params;
      const { status, limit = '50', offset = '0' } = request.query;
      const tenantId = request.tenant!.id;

      // First verify webhook belongs to tenant
      const webhook = await prisma.webhookConfig.findFirst({
        where: { id, tenantId },
        select: { id: true },
      });

      if (!webhook) {
        return reply.status(404).send({
          error: 'Webhook not found',
          code: 'WEBHOOK_NOT_FOUND',
        });
      }

      const where = {
        configId: id,
        ...(status && { status: status as any }),
      };

      const [deliveries, total] = await Promise.all([
        prisma.webhookDelivery.findMany({
          where,
          select: {
            id: true,
            status: true,
            attempts: true,
            lastAttempt: true,
            nextAttempt: true,
            response: true,
            createdAt: true,
            updatedAt: true,
            // Exclude payload for performance/privacy
          },
          skip: parseInt(offset),
          take: parseInt(limit),
          orderBy: { createdAt: 'desc' },
        }),
        prisma.webhookDelivery.count({ where }),
      ]);

      return {
        data: deliveries,
        meta: {
          total,
          limit: parseInt(limit),
          offset: parseInt(offset),
        },
      };
    } catch (error) {
      request.log.error('Error listing webhook deliveries:', error);
      return reply.status(500).send({
        error: 'Failed to list webhook deliveries',
        code: 'LIST_WEBHOOK_DELIVERIES_ERROR',
      });
    }
  });

  // POST /api/webhooks/:id/test - Test webhook
  fastify.post('/:id/test', async (request: GetWebhookRequest, reply: FastifyReply) => {
    try {
      const { id } = request.params;
      const tenantId = request.tenant!.id;

      // Verify webhook belongs to tenant
      const webhook = await prisma.webhookConfig.findFirst({
        where: { id, tenantId },
      });

      if (!webhook) {
        return reply.status(404).send({
          error: 'Webhook not found',
          code: 'WEBHOOK_NOT_FOUND',
        });
      }

      // Create a test webhook delivery
      const testPayload = {
        event: 'webhook.test',
        timestamp: new Date().toISOString(),
        data: {
          message: 'This is a test webhook delivery',
        },
      };

      const delivery = await prisma.webhookDelivery.create({
        data: {
          configId: id,
          payload: testPayload,
          status: 'PENDING',
        },
      });

      // TODO: Queue for delivery by webhook worker

      return { 
        success: true, 
        deliveryId: delivery.id,
        message: 'Test webhook queued for delivery'
      };
    } catch (error) {
      request.log.error('Error testing webhook:', error);
      return reply.status(500).send({
        error: 'Failed to test webhook',
        code: 'TEST_WEBHOOK_ERROR',
      });
    }
  });
};