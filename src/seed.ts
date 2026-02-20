import { PrismaClient } from '@prisma/client';
import * as crypto from 'crypto';

const prisma = new PrismaClient();

async function main() {
  console.log('🌱 Seeding database...');

  try {
    // Create test tenant
    console.log('Creating test tenant...');
    const tenant = await prisma.tenant.create({
      data: {
        name: 'Acme Corporation',
        apiKey: crypto.randomUUID(),
      },
    });
    console.log(`✅ Created tenant: ${tenant.name} (ID: ${tenant.id})`);

    // Create Quinn's 3 mailboxes
    console.log('Creating Quinn\'s mailboxes...');
    const mailboxes = await Promise.all([
      prisma.mailbox.create({
        data: {
          email: 'quinn.sales@acme.com',
          displayName: 'Quinn Anderson',
          status: 'ACTIVE',
          dailySendLimit: 50,
          weight: 2,
          tenantId: tenant.id,
        },
      }),
      prisma.mailbox.create({
        data: {
          email: 'quinn.outreach@acme.com',
          displayName: 'Quinn A.',
          status: 'ACTIVE',
          dailySendLimit: 30,
          weight: 1,
          tenantId: tenant.id,
        },
      }),
      prisma.mailbox.create({
        data: {
          email: 'quinn.followup@acme.com',
          displayName: 'Quinn Anderson',
          status: 'ACTIVE',
          dailySendLimit: 40,
          weight: 3,
          tenantId: tenant.id,
        },
      }),
    ]);

    console.log(`✅ Created ${mailboxes.length} mailboxes for Quinn`);
    mailboxes.forEach(mb => {
      console.log(`   - ${mb.email} (weight: ${mb.weight}, limit: ${mb.dailySendLimit})`);
    });

    // Create test sequence
    console.log('Creating test sequence...');
    const sequence = await prisma.sequence.create({
      data: {
        name: 'Welcome Series - New Prospects',
        description: 'A 3-step welcome sequence for new prospects',
        status: 'ACTIVE',
        tenantId: tenant.id,
        steps: {
          create: [
            {
              stepNumber: 1,
              subject: 'Welcome {{contact.firstName}}! Let\'s get started',
              body: `Hi {{contact.firstName}},

Welcome to {{sequence.name}}! 

I'm {{sender.firstName}} from {{sender.name}}, and I'm excited to help you get the most out of our platform.

Over the next few days, I'll be sending you some helpful resources to get you started:

• Step 1: Platform overview and key features
• Step 2: Best practices from successful customers  
• Step 3: Advanced tips and next steps

Let me know if you have any questions!

Best regards,
{{sender.firstName}}
{{sender.name}}`,
              delayHours: 0, // Send immediately
            },
            {
              stepNumber: 2,
              subject: 'Quick question about your goals, {{contact.firstName}}',
              body: `Hi {{contact.firstName}},

Yesterday I sent you an overview of our platform. I hope you found it helpful!

I wanted to quickly follow up and ask: what's your primary goal with our platform? Are you looking to:

1. Increase sales efficiency
2. Improve customer engagement  
3. Streamline your workflow
4. Something else entirely?

Understanding your goals will help me provide more relevant resources.

Looking forward to hearing from you!

{{sender.firstName}}`,
              delayHours: 24,
            },
            {
              stepNumber: 3,
              subject: 'Final step: Advanced strategies for {{contact.firstName}}',
              body: `Hi {{contact.firstName}},

This is the final email in our welcome series. I hope you've found the previous messages valuable!

Here are some advanced strategies that our most successful customers use:

• Automation workflows that save 5+ hours per week
• Integration with popular tools like Slack and Salesforce
• Custom reporting and analytics setup

If you're ready to implement any of these strategies, I'm here to help. Just reply to this email or schedule a quick 15-minute call.

Otherwise, you'll continue to receive our weekly newsletter with tips, case studies, and feature updates.

Thanks for joining us!

{{sender.firstName}}
{{sender.name}}`,
              delayHours: 72, // 3 days after step 2
            },
          ],
        },
      },
      include: {
        steps: true,
      },
    });

    console.log(`✅ Created sequence: ${sequence.name}`);
    console.log(`   - ${sequence.steps.length} steps`);

    // Create some test enrollments
    console.log('Creating test enrollments...');
    const testContacts = [
      { email: 'alice@example.com', name: 'Alice Johnson' },
      { email: 'bob@example.com', name: 'Bob Smith' },
      { email: 'charlie@example.com', name: 'Charlie Brown' },
    ];

    for (const contact of testContacts) {
      const enrollment = await prisma.sequenceEnrollment.create({
        data: {
          contactEmail: contact.email,
          contactName: contact.name,
          sequenceId: sequence.id,
          status: 'ACTIVE',
          currentStep: 1,
          steps: {
            create: sequence.steps.map((step, index) => ({
              stepId: step.id,
              status: index === 0 ? 'PENDING' : 'PENDING',
              scheduledAt: index === 0 ? new Date() : undefined,
            })),
          },
        },
      });

      console.log(`   - Enrolled ${contact.name} (${contact.email})`);
    }

    // Create webhook configuration
    console.log('Creating webhook configuration...');
    const webhook = await prisma.webhookConfig.create({
      data: {
        url: 'https://api.example.com/webhooks/sequence-events',
        secret: crypto.randomBytes(32).toString('hex'),
        events: [
          'email.sent',
          'email.bounced',
          'signal.detected',
          'circuit_breaker.triggered',
          'enrollment.completed',
        ],
        active: true,
        tenantId: tenant.id,
      },
    });

    console.log(`✅ Created webhook config: ${webhook.url}`);

    // Summary
    console.log('\n🎉 Database seeded successfully!');
    console.log('\nTest Data Summary:');
    console.log(`Tenant: ${tenant.name}`);
    console.log(`API Key: ${tenant.apiKey}`);
    console.log(`Mailboxes: ${mailboxes.length}`);
    console.log(`Sequence: ${sequence.name} (${sequence.steps.length} steps)`);
    console.log(`Enrollments: ${testContacts.length}`);
    console.log(`Webhooks: 1 active configuration`);
    
    console.log('\n📋 Next steps:');
    console.log('1. Start the development server: npm run dev');
    console.log('2. Test API endpoints with the API key above');
    console.log('3. Check the BullMQ dashboard for worker activity');
    console.log('4. Monitor webhook deliveries and sequence processing');

  } catch (error) {
    console.error('❌ Seeding failed:', error);
    throw error;
  } finally {
    await prisma.$disconnect();
  }
}

main()
  .catch((error) => {
    console.error(error);
    process.exit(1);
  });