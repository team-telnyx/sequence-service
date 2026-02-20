import { FastifyPluginAsync, FastifyRequest, FastifyReply } from 'fastify';
import { z } from 'zod';

// Request schemas
const createMailboxSchema = z.object({
  email: z.string().email(),
  displayName: z.string().optional(),
  dailySendLimit: z.number().int().positive().default(50),
  weight: z.number().int().positive().default(1),
});

const updateMailboxSchema = z.object({
  displayName: z.string().optional(),
  status: z.enum(['ACTIVE', 'PAUSED', 'OUT_OF_OFFICE', 'BOUNCED', 'DISABLED']).optional(),
  dailySendLimit: z.number().int().positive().optional(),
  weight: z.number().int().positive().optional(),
});

type CreateMailboxRequest = FastifyRequest<{
  Body: z.infer<typeof createMailboxSchema>;
}>;

type UpdateMailboxRequest = FastifyRequest<{
  Params: { id: string };
  Body: z.infer<typeof updateMailboxSchema>;
}>;

type GetMailboxRequest = FastifyRequest<{
  Params: { id: string };
}>;

type ListMailboxesRequest = FastifyRequest<{
  Querystring: {
    status?: string;
    limit?: string;
    offset?: string;
  };
}>;

export const mailboxRoutes: FastifyPluginAsync = async (fastify) => {
  const { prisma } = fastify;

  // GET /api/mailboxes - List mailboxes
  fastify.get('/', async (request: ListMailboxesRequest, reply: FastifyReply) => {
    try {
      const { status, limit = '50', offset = '0' } = request.query;
      const tenantId = request.tenant!.id;

      const where = {
        tenantId,
        ...(status && { status: status as any }),
      };

      const [mailboxes, total] = await Promise.all([
        prisma.mailbox.findMany({
          where,
          select: {
            id: true,
            email: true,
            displayName: true,
            status: true,
            dailySendLimit: true,
            sentToday: true,
            weight: true,
            createdAt: true,
            updatedAt: true,
            // Exclude sensitive fields like tokens
          },
          skip: parseInt(offset),
          take: parseInt(limit),
          orderBy: { createdAt: 'desc' },
        }),
        prisma.mailbox.count({ where }),
      ]);

      return {
        data: mailboxes,
        meta: {
          total,
          limit: parseInt(limit),
          offset: parseInt(offset),
        },
      };
    } catch (error) {
      request.log.error('Error listing mailboxes:', error);
      return reply.status(500).send({
        error: 'Failed to list mailboxes',
        code: 'LIST_MAILBOXES_ERROR',
      });
    }
  });

  // GET /api/mailboxes/:id - Get mailbox
  fastify.get('/:id', async (request: GetMailboxRequest, reply: FastifyReply) => {
    try {
      const { id } = request.params;
      const tenantId = request.tenant!.id;

      const mailbox = await prisma.mailbox.findFirst({
        where: { id, tenantId },
        select: {
          id: true,
          email: true,
          displayName: true,
          status: true,
          dailySendLimit: true,
          sentToday: true,
          weight: true,
          createdAt: true,
          updatedAt: true,
          // Exclude sensitive fields like tokens
        },
      });

      if (!mailbox) {
        return reply.status(404).send({
          error: 'Mailbox not found',
          code: 'MAILBOX_NOT_FOUND',
        });
      }

      return { data: mailbox };
    } catch (error) {
      request.log.error('Error getting mailbox:', error);
      return reply.status(500).send({
        error: 'Failed to get mailbox',
        code: 'GET_MAILBOX_ERROR',
      });
    }
  });

  // POST /api/mailboxes - Create mailbox
  fastify.post('/', async (request: CreateMailboxRequest, reply: FastifyReply) => {
    try {
      const body = createMailboxSchema.parse(request.body);
      const tenantId = request.tenant!.id;

      const mailbox = await prisma.mailbox.create({
        data: {
          email: body.email,
          displayName: body.displayName,
          dailySendLimit: body.dailySendLimit,
          weight: body.weight,
          tenantId,
        },
        select: {
          id: true,
          email: true,
          displayName: true,
          status: true,
          dailySendLimit: true,
          sentToday: true,
          weight: true,
          createdAt: true,
          updatedAt: true,
        },
      });

      return reply.status(201).send({ data: mailbox });
    } catch (error) {
      if (error instanceof z.ZodError) {
        return reply.status(400).send({
          error: 'Validation error',
          details: error.errors,
          code: 'VALIDATION_ERROR',
        });
      }
      
      request.log.error('Error creating mailbox:', error);
      return reply.status(500).send({
        error: 'Failed to create mailbox',
        code: 'CREATE_MAILBOX_ERROR',
      });
    }
  });

  // PUT /api/mailboxes/:id - Update mailbox
  fastify.put('/:id', async (request: UpdateMailboxRequest, reply: FastifyReply) => {
    try {
      const { id } = request.params;
      const body = updateMailboxSchema.parse(request.body);
      const tenantId = request.tenant!.id;

      const mailbox = await prisma.mailbox.updateMany({
        where: { id, tenantId },
        data: body,
      });

      if (mailbox.count === 0) {
        return reply.status(404).send({
          error: 'Mailbox not found',
          code: 'MAILBOX_NOT_FOUND',
        });
      }

      const updatedMailbox = await prisma.mailbox.findFirst({
        where: { id, tenantId },
        select: {
          id: true,
          email: true,
          displayName: true,
          status: true,
          dailySendLimit: true,
          sentToday: true,
          weight: true,
          createdAt: true,
          updatedAt: true,
        },
      });

      return { data: updatedMailbox };
    } catch (error) {
      if (error instanceof z.ZodError) {
        return reply.status(400).send({
          error: 'Validation error',
          details: error.errors,
          code: 'VALIDATION_ERROR',
        });
      }
      
      request.log.error('Error updating mailbox:', error);
      return reply.status(500).send({
        error: 'Failed to update mailbox',
        code: 'UPDATE_MAILBOX_ERROR',
      });
    }
  });

  // DELETE /api/mailboxes/:id - Delete mailbox
  fastify.delete('/:id', async (request: GetMailboxRequest, reply: FastifyReply) => {
    try {
      const { id } = request.params;
      const tenantId = request.tenant!.id;

      const mailbox = await prisma.mailbox.deleteMany({
        where: { id, tenantId },
      });

      if (mailbox.count === 0) {
        return reply.status(404).send({
          error: 'Mailbox not found',
          code: 'MAILBOX_NOT_FOUND',
        });
      }

      return reply.status(204).send();
    } catch (error) {
      request.log.error('Error deleting mailbox:', error);
      return reply.status(500).send({
        error: 'Failed to delete mailbox',
        code: 'DELETE_MAILBOX_ERROR',
      });
    }
  });

  // POST /api/mailboxes/:id/reset-sent-today - Reset sentToday counter
  fastify.post('/:id/reset-sent-today', async (request: GetMailboxRequest, reply: FastifyReply) => {
    try {
      const { id } = request.params;
      const tenantId = request.tenant!.id;

      const mailbox = await prisma.mailbox.updateMany({
        where: { id, tenantId },
        data: { sentToday: 0 },
      });

      if (mailbox.count === 0) {
        return reply.status(404).send({
          error: 'Mailbox not found',
          code: 'MAILBOX_NOT_FOUND',
        });
      }

      return { success: true };
    } catch (error) {
      request.log.error('Error resetting sentToday:', error);
      return reply.status(500).send({
        error: 'Failed to reset sentToday counter',
        code: 'RESET_SENT_TODAY_ERROR',
      });
    }
  });
};