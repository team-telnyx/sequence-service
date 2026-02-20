import { FastifyPluginAsync, FastifyRequest, FastifyReply } from 'fastify';
import { z } from 'zod';

// Request schemas
const createEnrollmentSchema = z.object({
  sequenceId: z.string().cuid(),
  contactEmail: z.string().email(),
  contactName: z.string().optional(),
});

const updateEnrollmentSchema = z.object({
  status: z.enum(['ACTIVE', 'PAUSED', 'COMPLETED', 'BOUNCED', 'UNSUBSCRIBED']).optional(),
  currentStep: z.number().int().positive().optional(),
});

type CreateEnrollmentRequest = FastifyRequest<{
  Body: z.infer<typeof createEnrollmentSchema>;
}>;

type UpdateEnrollmentRequest = FastifyRequest<{
  Params: { id: string };
  Body: z.infer<typeof updateEnrollmentSchema>;
}>;

type GetEnrollmentRequest = FastifyRequest<{
  Params: { id: string };
}>;

type ListEnrollmentsRequest = FastifyRequest<{
  Querystring: {
    sequenceId?: string;
    status?: string;
    contactEmail?: string;
    limit?: string;
    offset?: string;
  };
}>;

export const enrollmentRoutes: FastifyPluginAsync = async (fastify) => {
  const { prisma } = fastify;

  // GET /api/enrollments - List enrollments
  fastify.get('/', async (request: ListEnrollmentsRequest, reply: FastifyReply) => {
    try {
      const { sequenceId, status, contactEmail, limit = '50', offset = '0' } = request.query;
      const tenantId = request.tenant!.id;

      // Build where clause with tenant scope
      const where: any = {
        sequence: {
          tenantId,
        },
      };

      if (sequenceId) where.sequenceId = sequenceId;
      if (status) where.status = status;
      if (contactEmail) where.contactEmail = { contains: contactEmail, mode: 'insensitive' };

      const [enrollments, total] = await Promise.all([
        prisma.sequenceEnrollment.findMany({
          where,
          include: {
            sequence: {
              select: {
                id: true,
                name: true,
                status: true,
              },
            },
            steps: {
              include: {
                step: {
                  select: {
                    stepNumber: true,
                    subject: true,
                  },
                },
                mailbox: {
                  select: {
                    id: true,
                    email: true,
                    displayName: true,
                  },
                },
              },
              orderBy: {
                step: {
                  stepNumber: 'asc',
                },
              },
            },
          },
          skip: parseInt(offset),
          take: parseInt(limit),
          orderBy: { createdAt: 'desc' },
        }),
        prisma.sequenceEnrollment.count({ where }),
      ]);

      return {
        data: enrollments,
        meta: {
          total,
          limit: parseInt(limit),
          offset: parseInt(offset),
        },
      };
    } catch (error) {
      request.log.error('Error listing enrollments:', error);
      return reply.status(500).send({
        error: 'Failed to list enrollments',
        code: 'LIST_ENROLLMENTS_ERROR',
      });
    }
  });

  // GET /api/enrollments/:id - Get enrollment
  fastify.get('/:id', async (request: GetEnrollmentRequest, reply: FastifyReply) => {
    try {
      const { id } = request.params;
      const tenantId = request.tenant!.id;

      const enrollment = await prisma.sequenceEnrollment.findFirst({
        where: {
          id,
          sequence: {
            tenantId,
          },
        },
        include: {
          sequence: {
            include: {
              steps: {
                orderBy: { stepNumber: 'asc' },
              },
            },
          },
          steps: {
            include: {
              step: true,
              mailbox: {
                select: {
                  id: true,
                  email: true,
                  displayName: true,
                },
              },
              sentEmails: {
                select: {
                  id: true,
                  messageId: true,
                  subject: true,
                  sentAt: true,
                  signals: {
                    select: {
                      type: true,
                      detectedAt: true,
                    },
                  },
                },
              },
            },
            orderBy: {
              step: {
                stepNumber: 'asc',
              },
            },
          },
        },
      });

      if (!enrollment) {
        return reply.status(404).send({
          error: 'Enrollment not found',
          code: 'ENROLLMENT_NOT_FOUND',
        });
      }

      return { data: enrollment };
    } catch (error) {
      request.log.error('Error getting enrollment:', error);
      return reply.status(500).send({
        error: 'Failed to get enrollment',
        code: 'GET_ENROLLMENT_ERROR',
      });
    }
  });

  // POST /api/enrollments - Create enrollment
  fastify.post('/', async (request: CreateEnrollmentRequest, reply: FastifyReply) => {
    try {
      const body = createEnrollmentSchema.parse(request.body);
      const tenantId = request.tenant!.id;

      // Verify sequence belongs to tenant
      const sequence = await prisma.sequence.findFirst({
        where: {
          id: body.sequenceId,
          tenantId,
        },
        include: {
          steps: {
            orderBy: { stepNumber: 'asc' },
          },
        },
      });

      if (!sequence) {
        return reply.status(404).send({
          error: 'Sequence not found',
          code: 'SEQUENCE_NOT_FOUND',
        });
      }

      // Check if enrollment already exists
      const existingEnrollment = await prisma.sequenceEnrollment.findUnique({
        where: {
          sequenceId_contactEmail: {
            sequenceId: body.sequenceId,
            contactEmail: body.contactEmail,
          },
        },
      });

      if (existingEnrollment) {
        return reply.status(409).send({
          error: 'Contact already enrolled in this sequence',
          code: 'ENROLLMENT_EXISTS',
        });
      }

      const enrollment = await prisma.sequenceEnrollment.create({
        data: {
          sequenceId: body.sequenceId,
          contactEmail: body.contactEmail,
          contactName: body.contactName,
          steps: {
            create: sequence.steps.map((step) => ({
              stepId: step.id,
              status: step.stepNumber === 1 ? 'PENDING' : 'PENDING',
            })),
          },
        },
        include: {
          sequence: {
            select: {
              id: true,
              name: true,
              status: true,
            },
          },
          steps: {
            include: {
              step: {
                select: {
                  stepNumber: true,
                  subject: true,
                },
              },
            },
            orderBy: {
              step: {
                stepNumber: 'asc',
              },
            },
          },
        },
      });

      return reply.status(201).send({ data: enrollment });
    } catch (error) {
      if (error instanceof z.ZodError) {
        return reply.status(400).send({
          error: 'Validation error',
          details: error.errors,
          code: 'VALIDATION_ERROR',
        });
      }
      
      request.log.error('Error creating enrollment:', error);
      return reply.status(500).send({
        error: 'Failed to create enrollment',
        code: 'CREATE_ENROLLMENT_ERROR',
      });
    }
  });

  // PUT /api/enrollments/:id - Update enrollment
  fastify.put('/:id', async (request: UpdateEnrollmentRequest, reply: FastifyReply) => {
    try {
      const { id } = request.params;
      const body = updateEnrollmentSchema.parse(request.body);
      const tenantId = request.tenant!.id;

      const enrollment = await prisma.sequenceEnrollment.updateMany({
        where: {
          id,
          sequence: {
            tenantId,
          },
        },
        data: body,
      });

      if (enrollment.count === 0) {
        return reply.status(404).send({
          error: 'Enrollment not found',
          code: 'ENROLLMENT_NOT_FOUND',
        });
      }

      const updatedEnrollment = await prisma.sequenceEnrollment.findFirst({
        where: {
          id,
          sequence: {
            tenantId,
          },
        },
        include: {
          sequence: {
            select: {
              id: true,
              name: true,
              status: true,
            },
          },
          steps: {
            include: {
              step: {
                select: {
                  stepNumber: true,
                  subject: true,
                },
              },
            },
            orderBy: {
              step: {
                stepNumber: 'asc',
              },
            },
          },
        },
      });

      return { data: updatedEnrollment };
    } catch (error) {
      if (error instanceof z.ZodError) {
        return reply.status(400).send({
          error: 'Validation error',
          details: error.errors,
          code: 'VALIDATION_ERROR',
        });
      }
      
      request.log.error('Error updating enrollment:', error);
      return reply.status(500).send({
        error: 'Failed to update enrollment',
        code: 'UPDATE_ENROLLMENT_ERROR',
      });
    }
  });

  // DELETE /api/enrollments/:id - Delete enrollment
  fastify.delete('/:id', async (request: GetEnrollmentRequest, reply: FastifyReply) => {
    try {
      const { id } = request.params;
      const tenantId = request.tenant!.id;

      const enrollment = await prisma.sequenceEnrollment.deleteMany({
        where: {
          id,
          sequence: {
            tenantId,
          },
        },
      });

      if (enrollment.count === 0) {
        return reply.status(404).send({
          error: 'Enrollment not found',
          code: 'ENROLLMENT_NOT_FOUND',
        });
      }

      return reply.status(204).send();
    } catch (error) {
      request.log.error('Error deleting enrollment:', error);
      return reply.status(500).send({
        error: 'Failed to delete enrollment',
        code: 'DELETE_ENROLLMENT_ERROR',
      });
    }
  });

  // POST /api/enrollments/:id/pause - Pause enrollment
  fastify.post('/:id/pause', async (request: GetEnrollmentRequest, reply: FastifyReply) => {
    try {
      const { id } = request.params;
      const tenantId = request.tenant!.id;

      const enrollment = await prisma.sequenceEnrollment.updateMany({
        where: {
          id,
          sequence: {
            tenantId,
          },
        },
        data: { status: 'PAUSED' },
      });

      if (enrollment.count === 0) {
        return reply.status(404).send({
          error: 'Enrollment not found',
          code: 'ENROLLMENT_NOT_FOUND',
        });
      }

      return { success: true };
    } catch (error) {
      request.log.error('Error pausing enrollment:', error);
      return reply.status(500).send({
        error: 'Failed to pause enrollment',
        code: 'PAUSE_ENROLLMENT_ERROR',
      });
    }
  });

  // POST /api/enrollments/:id/resume - Resume enrollment
  fastify.post('/:id/resume', async (request: GetEnrollmentRequest, reply: FastifyReply) => {
    try {
      const { id } = request.params;
      const tenantId = request.tenant!.id;

      const enrollment = await prisma.sequenceEnrollment.updateMany({
        where: {
          id,
          sequence: {
            tenantId,
          },
        },
        data: { status: 'ACTIVE' },
      });

      if (enrollment.count === 0) {
        return reply.status(404).send({
          error: 'Enrollment not found',
          code: 'ENROLLMENT_NOT_FOUND',
        });
      }

      return { success: true };
    } catch (error) {
      request.log.error('Error resuming enrollment:', error);
      return reply.status(500).send({
        error: 'Failed to resume enrollment',
        code: 'RESUME_ENROLLMENT_ERROR',
      });
    }
  });
};