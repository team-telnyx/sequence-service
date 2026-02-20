import { FastifyPluginAsync, FastifyRequest, FastifyReply } from 'fastify';
import { z } from 'zod';

// Request schemas
const createSignalSchema = z.object({
  type: z.enum(['REPLY', 'CLICK', 'OPEN', 'BOUNCE', 'OUT_OF_OFFICE', 'UNSUBSCRIBE']),
  mailboxId: z.string().cuid(),
  sentEmailId: z.string().cuid().optional(),
  data: z.any().optional(),
});

type CreateSignalRequest = FastifyRequest<{
  Body: z.infer<typeof createSignalSchema>;
}>;

type GetSignalRequest = FastifyRequest<{
  Params: { id: string };
}>;

type ListSignalsRequest = FastifyRequest<{
  Querystring: {
    mailboxId?: string;
    sentEmailId?: string;
    type?: string;
    limit?: string;
    offset?: string;
    from?: string;
    to?: string;
  };
}>;

export const signalRoutes: FastifyPluginAsync = async (fastify) => {
  const { prisma } = fastify;

  // GET /api/signals - List signals
  fastify.get('/', async (request: ListSignalsRequest, reply: FastifyReply) => {
    try {
      const { 
        mailboxId, 
        sentEmailId, 
        type, 
        limit = '50', 
        offset = '0',
        from,
        to
      } = request.query;
      const tenantId = request.tenant!.id;

      // Build where clause with tenant scope
      const where: any = {
        mailbox: {
          tenantId,
        },
      };

      if (mailboxId) where.mailboxId = mailboxId;
      if (sentEmailId) where.sentEmailId = sentEmailId;
      if (type) where.type = type;
      
      // Date range filtering
      if (from || to) {
        where.detectedAt = {};
        if (from) where.detectedAt.gte = new Date(from);
        if (to) where.detectedAt.lte = new Date(to);
      }

      const [signals, total] = await Promise.all([
        prisma.signal.findMany({
          where,
          include: {
            mailbox: {
              select: {
                id: true,
                email: true,
                displayName: true,
              },
            },
            sentEmail: {
              select: {
                id: true,
                messageId: true,
                subject: true,
                toEmail: true,
                sentAt: true,
              },
            },
          },
          skip: parseInt(offset),
          take: parseInt(limit),
          orderBy: { detectedAt: 'desc' },
        }),
        prisma.signal.count({ where }),
      ]);

      return {
        data: signals,
        meta: {
          total,
          limit: parseInt(limit),
          offset: parseInt(offset),
        },
      };
    } catch (error) {
      request.log.error('Error listing signals:', error);
      return reply.status(500).send({
        error: 'Failed to list signals',
        code: 'LIST_SIGNALS_ERROR',
      });
    }
  });

  // GET /api/signals/:id - Get signal
  fastify.get('/:id', async (request: GetSignalRequest, reply: FastifyReply) => {
    try {
      const { id } = request.params;
      const tenantId = request.tenant!.id;

      const signal = await prisma.signal.findFirst({
        where: {
          id,
          mailbox: {
            tenantId,
          },
        },
        include: {
          mailbox: {
            select: {
              id: true,
              email: true,
              displayName: true,
            },
          },
          sentEmail: {
            select: {
              id: true,
              messageId: true,
              subject: true,
              body: true,
              toEmail: true,
              toName: true,
              fromEmail: true,
              fromName: true,
              sentAt: true,
              enrollmentStep: {
                select: {
                  id: true,
                  enrollment: {
                    select: {
                      id: true,
                      contactEmail: true,
                      contactName: true,
                      sequence: {
                        select: {
                          id: true,
                          name: true,
                        },
                      },
                    },
                  },
                  step: {
                    select: {
                      id: true,
                      stepNumber: true,
                      subject: true,
                    },
                  },
                },
              },
            },
          },
        },
      });

      if (!signal) {
        return reply.status(404).send({
          error: 'Signal not found',
          code: 'SIGNAL_NOT_FOUND',
        });
      }

      return { data: signal };
    } catch (error) {
      request.log.error('Error getting signal:', error);
      return reply.status(500).send({
        error: 'Failed to get signal',
        code: 'GET_SIGNAL_ERROR',
      });
    }
  });

  // POST /api/signals - Create signal
  fastify.post('/', async (request: CreateSignalRequest, reply: FastifyReply) => {
    try {
      const body = createSignalSchema.parse(request.body);
      const tenantId = request.tenant!.id;

      // Verify mailbox belongs to tenant
      const mailbox = await prisma.mailbox.findFirst({
        where: {
          id: body.mailboxId,
          tenantId,
        },
      });

      if (!mailbox) {
        return reply.status(404).send({
          error: 'Mailbox not found',
          code: 'MAILBOX_NOT_FOUND',
        });
      }

      // If sentEmailId is provided, verify it belongs to the mailbox
      if (body.sentEmailId) {
        const sentEmail = await prisma.sentEmail.findFirst({
          where: {
            id: body.sentEmailId,
            mailboxId: body.mailboxId,
          },
        });

        if (!sentEmail) {
          return reply.status(404).send({
            error: 'Sent email not found for this mailbox',
            code: 'SENT_EMAIL_NOT_FOUND',
          });
        }
      }

      const signal = await prisma.signal.create({
        data: {
          type: body.type,
          mailboxId: body.mailboxId,
          sentEmailId: body.sentEmailId,
          data: body.data,
        },
        include: {
          mailbox: {
            select: {
              id: true,
              email: true,
              displayName: true,
            },
          },
          sentEmail: {
            select: {
              id: true,
              messageId: true,
              subject: true,
              toEmail: true,
              sentAt: true,
            },
          },
        },
      });

      // TODO: Process signal effects (pause sequences, update enrollment status, etc.)

      return reply.status(201).send({ data: signal });
    } catch (error) {
      if (error instanceof z.ZodError) {
        return reply.status(400).send({
          error: 'Validation error',
          details: error.errors,
          code: 'VALIDATION_ERROR',
        });
      }
      
      request.log.error('Error creating signal:', error);
      return reply.status(500).send({
        error: 'Failed to create signal',
        code: 'CREATE_SIGNAL_ERROR',
      });
    }
  });

  // GET /api/signals/stats - Get signal statistics
  fastify.get('/stats', async (request: ListSignalsRequest, reply: FastifyReply) => {
    try {
      const { 
        mailboxId,
        from,
        to
      } = request.query;
      const tenantId = request.tenant!.id;

      // Build where clause with tenant scope
      const where: any = {
        mailbox: {
          tenantId,
        },
      };

      if (mailboxId) where.mailboxId = mailboxId;
      
      // Date range filtering
      if (from || to) {
        where.detectedAt = {};
        if (from) where.detectedAt.gte = new Date(from);
        if (to) where.detectedAt.lte = new Date(to);
      }

      // Get signal counts by type
      const signalCounts = await prisma.signal.groupBy({
        by: ['type'],
        where,
        _count: {
          _all: true,
        },
      });

      // Calculate totals
      const stats = {
        total: signalCounts.reduce((sum, item) => sum + item._count._all, 0),
        byType: signalCounts.reduce((acc, item) => {
          acc[item.type] = item._count._all;
          return acc;
        }, {} as Record<string, number>),
      };

      // Calculate rates if we have email data
      if (stats.total > 0) {
        // Get total emails sent in the same period
        const emailWhere: any = {
          mailbox: {
            tenantId,
          },
        };

        if (mailboxId) emailWhere.mailboxId = mailboxId;
        
        if (from || to) {
          emailWhere.sentAt = {};
          if (from) emailWhere.sentAt.gte = new Date(from);
          if (to) emailWhere.sentAt.lte = new Date(to);
        }

        const totalEmails = await prisma.sentEmail.count({
          where: emailWhere,
        });

        if (totalEmails > 0) {
          stats.rates = {
            reply: ((stats.byType.REPLY || 0) / totalEmails * 100).toFixed(2),
            open: ((stats.byType.OPEN || 0) / totalEmails * 100).toFixed(2),
            click: ((stats.byType.CLICK || 0) / totalEmails * 100).toFixed(2),
            bounce: ((stats.byType.BOUNCE || 0) / totalEmails * 100).toFixed(2),
            unsubscribe: ((stats.byType.UNSUBSCRIBE || 0) / totalEmails * 100).toFixed(2),
            outOfOffice: ((stats.byType.OUT_OF_OFFICE || 0) / totalEmails * 100).toFixed(2),
          };
        }
      }

      return { data: stats };
    } catch (error) {
      request.log.error('Error getting signal stats:', error);
      return reply.status(500).send({
        error: 'Failed to get signal statistics',
        code: 'GET_SIGNAL_STATS_ERROR',
      });
    }
  });

  // DELETE /api/signals/:id - Delete signal
  fastify.delete('/:id', async (request: GetSignalRequest, reply: FastifyReply) => {
    try {
      const { id } = request.params;
      const tenantId = request.tenant!.id;

      const signal = await prisma.signal.deleteMany({
        where: {
          id,
          mailbox: {
            tenantId,
          },
        },
      });

      if (signal.count === 0) {
        return reply.status(404).send({
          error: 'Signal not found',
          code: 'SIGNAL_NOT_FOUND',
        });
      }

      return reply.status(204).send();
    } catch (error) {
      request.log.error('Error deleting signal:', error);
      return reply.status(500).send({
        error: 'Failed to delete signal',
        code: 'DELETE_SIGNAL_ERROR',
      });
    }
  });
};