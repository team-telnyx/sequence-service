import { FastifyPluginAsync, FastifyRequest, FastifyReply } from 'fastify';
import { z } from 'zod';

// Request schemas
const createSequenceSchema = z.object({
  name: z.string().min(1),
  description: z.string().optional(),
  steps: z.array(
    z.object({
      stepNumber: z.number().int().positive(),
      subject: z.string().min(1),
      body: z.string().min(1),
      delayHours: z.number().int().min(0).default(24),
    })
  ),
});

const updateSequenceSchema = z.object({
  name: z.string().min(1).optional(),
  description: z.string().optional(),
  status: z.enum(['ACTIVE', 'PAUSED', 'ARCHIVED']).optional(),
});

type CreateSequenceRequest = FastifyRequest<{
  Body: z.infer<typeof createSequenceSchema>;
}>;

type UpdateSequenceRequest = FastifyRequest<{
  Params: { id: string };
  Body: z.infer<typeof updateSequenceSchema>;
}>;

type GetSequenceRequest = FastifyRequest<{
  Params: { id: string };
}>;

type ListSequencesRequest = FastifyRequest<{
  Querystring: {
    status?: string;
    limit?: string;
    offset?: string;
  };
}>;

export const sequenceRoutes: FastifyPluginAsync = async (fastify) => {
  const { prisma } = fastify;

  // GET /api/sequences - List sequences
  fastify.get('/', async (request: ListSequencesRequest, reply: FastifyReply) => {
    try {
      const { status, limit = '50', offset = '0' } = request.query;
      const tenantId = request.tenant!.id;

      const where = {
        tenantId,
        ...(status && { status: status as any }),
      };

      const [sequences, total] = await Promise.all([
        prisma.sequence.findMany({
          where,
          include: {
            steps: true,
            _count: {
              select: {
                enrollments: true,
              },
            },
          },
          skip: parseInt(offset),
          take: parseInt(limit),
          orderBy: { createdAt: 'desc' },
        }),
        prisma.sequence.count({ where }),
      ]);

      return {
        data: sequences,
        meta: {
          total,
          limit: parseInt(limit),
          offset: parseInt(offset),
        },
      };
    } catch (error) {
      request.log.error('Error listing sequences:', error);
      return reply.status(500).send({
        error: 'Failed to list sequences',
        code: 'LIST_SEQUENCES_ERROR',
      });
    }
  });

  // GET /api/sequences/:id - Get sequence
  fastify.get('/:id', async (request: GetSequenceRequest, reply: FastifyReply) => {
    try {
      const { id } = request.params;
      const tenantId = request.tenant!.id;

      const sequence = await prisma.sequence.findFirst({
        where: { id, tenantId },
        include: {
          steps: {
            orderBy: { stepNumber: 'asc' },
          },
          _count: {
            select: {
              enrollments: true,
            },
          },
        },
      });

      if (!sequence) {
        return reply.status(404).send({
          error: 'Sequence not found',
          code: 'SEQUENCE_NOT_FOUND',
        });
      }

      return { data: sequence };
    } catch (error) {
      request.log.error('Error getting sequence:', error);
      return reply.status(500).send({
        error: 'Failed to get sequence',
        code: 'GET_SEQUENCE_ERROR',
      });
    }
  });

  // POST /api/sequences - Create sequence
  fastify.post('/', async (request: CreateSequenceRequest, reply: FastifyReply) => {
    try {
      const body = createSequenceSchema.parse(request.body);
      const tenantId = request.tenant!.id;

      const sequence = await prisma.sequence.create({
        data: {
          name: body.name,
          description: body.description,
          tenantId,
          steps: {
            create: body.steps.map((step) => ({
              stepNumber: step.stepNumber,
              subject: step.subject,
              body: step.body,
              delayHours: step.delayHours,
            })),
          },
        },
        include: {
          steps: {
            orderBy: { stepNumber: 'asc' },
          },
        },
      });

      return reply.status(201).send({ data: sequence });
    } catch (error) {
      if (error instanceof z.ZodError) {
        return reply.status(400).send({
          error: 'Validation error',
          details: error.errors,
          code: 'VALIDATION_ERROR',
        });
      }
      
      request.log.error('Error creating sequence:', error);
      return reply.status(500).send({
        error: 'Failed to create sequence',
        code: 'CREATE_SEQUENCE_ERROR',
      });
    }
  });

  // PUT /api/sequences/:id - Update sequence
  fastify.put('/:id', async (request: UpdateSequenceRequest, reply: FastifyReply) => {
    try {
      const { id } = request.params;
      const body = updateSequenceSchema.parse(request.body);
      const tenantId = request.tenant!.id;

      const sequence = await prisma.sequence.updateMany({
        where: { id, tenantId },
        data: body,
      });

      if (sequence.count === 0) {
        return reply.status(404).send({
          error: 'Sequence not found',
          code: 'SEQUENCE_NOT_FOUND',
        });
      }

      const updatedSequence = await prisma.sequence.findFirst({
        where: { id, tenantId },
        include: {
          steps: {
            orderBy: { stepNumber: 'asc' },
          },
        },
      });

      return { data: updatedSequence };
    } catch (error) {
      if (error instanceof z.ZodError) {
        return reply.status(400).send({
          error: 'Validation error',
          details: error.errors,
          code: 'VALIDATION_ERROR',
        });
      }
      
      request.log.error('Error updating sequence:', error);
      return reply.status(500).send({
        error: 'Failed to update sequence',
        code: 'UPDATE_SEQUENCE_ERROR',
      });
    }
  });

  // DELETE /api/sequences/:id - Delete sequence
  fastify.delete('/:id', async (request: GetSequenceRequest, reply: FastifyReply) => {
    try {
      const { id } = request.params;
      const tenantId = request.tenant!.id;

      const sequence = await prisma.sequence.deleteMany({
        where: { id, tenantId },
      });

      if (sequence.count === 0) {
        return reply.status(404).send({
          error: 'Sequence not found',
          code: 'SEQUENCE_NOT_FOUND',
        });
      }

      return reply.status(204).send();
    } catch (error) {
      request.log.error('Error deleting sequence:', error);
      return reply.status(500).send({
        error: 'Failed to delete sequence',
        code: 'DELETE_SEQUENCE_ERROR',
      });
    }
  });
};