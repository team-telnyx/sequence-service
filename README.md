# Sequence Service

A multi-tenant email sequence service built with TypeScript, Fastify, Prisma, and BullMQ.

## Features

- **Multi-tenant Architecture**: Complete tenant isolation with API key authentication
- **Email Sequences**: Create and manage multi-step email campaigns
- **Mailbox Rotation**: Intelligent weighted selection with daily send limits
- **Gmail Integration**: Send emails via Gmail API (optional)
- **Signal Detection**: Automatic reply, bounce, and OOF detection
- **Webhook Delivery**: HMAC-signed webhook notifications with retry logic
- **Circuit Breaker**: Automatic pause on high bounce rates
- **Template Rendering**: Handlebars-based email personalization

## Tech Stack

- **Runtime**: Node.js 18+
- **Framework**: Fastify
- **Database**: PostgreSQL with Prisma ORM
- **Queue**: BullMQ with Redis
- **Templates**: Handlebars
- **Testing**: Vitest
- **Linting**: ESLint + Prettier
- **Type Safety**: TypeScript

## Quick Start

1. **Clone and Install**
   ```bash
   git clone <repo-url>
   cd sequence-service
   npm install
   ```

2. **Environment Setup**
   ```bash
   cp .env.example .env
   # Edit .env with your database and Redis URLs
   ```

3. **Database Setup**
   ```bash
   npm run docker:up
   npm run db:push
   npm run db:seed
   ```

4. **Start Development**
   ```bash
   npm run dev
   ```

The API will be available at `http://localhost:3000`

## Environment Variables

```bash
# Database
DATABASE_URL="postgresql://sequence_user:sequence_pass@localhost:5432/sequence_service"

# Redis
REDIS_URL="redis://localhost:6379"

# Server
PORT=3000
NODE_ENV=development

# Gmail Integration (optional)
GMAIL_ENABLED=false
GOOGLE_CLIENT_ID=your-client-id
GOOGLE_CLIENT_SECRET=your-client-secret
GOOGLE_REDIRECT_URI=your-redirect-uri

# Scout Mode (for testing)
SCOUT_STUB_MODE=true

# Webhook settings
WEBHOOK_SECRET=your-webhook-secret-here

# Workers
WORKER_CONCURRENCY=10
```

## API Documentation

### Authentication

All API endpoints require an `X-API-Key` header with a valid tenant API key.

```bash
curl -H "X-API-Key: your-tenant-api-key" http://localhost:3000/api/sequences
```

### Core Endpoints

#### Sequences
- `GET /api/sequences` - List sequences
- `POST /api/sequences` - Create sequence
- `GET /api/sequences/:id` - Get sequence
- `PUT /api/sequences/:id` - Update sequence
- `DELETE /api/sequences/:id` - Delete sequence

#### Mailboxes
- `GET /api/mailboxes` - List mailboxes
- `POST /api/mailboxes` - Create mailbox
- `GET /api/mailboxes/:id` - Get mailbox
- `PUT /api/mailboxes/:id` - Update mailbox
- `DELETE /api/mailboxes/:id` - Delete mailbox
- `POST /api/mailboxes/:id/reset-sent-today` - Reset daily counter

#### Enrollments
- `GET /api/enrollments` - List enrollments
- `POST /api/enrollments` - Create enrollment
- `GET /api/enrollments/:id` - Get enrollment
- `PUT /api/enrollments/:id` - Update enrollment
- `DELETE /api/enrollments/:id` - Delete enrollment
- `POST /api/enrollments/:id/pause` - Pause enrollment
- `POST /api/enrollments/:id/resume` - Resume enrollment

#### Webhooks
- `GET /api/webhooks` - List webhooks
- `POST /api/webhooks` - Create webhook
- `GET /api/webhooks/:id` - Get webhook
- `PUT /api/webhooks/:id` - Update webhook
- `DELETE /api/webhooks/:id` - Delete webhook
- `GET /api/webhooks/:id/deliveries` - List deliveries
- `POST /api/webhooks/:id/test` - Test webhook

#### Signals
- `GET /api/signals` - List signals
- `POST /api/signals` - Create signal
- `GET /api/signals/:id` - Get signal
- `GET /api/signals/stats` - Get signal statistics
- `DELETE /api/signals/:id` - Delete signal

### Example Requests

**Create a Sequence:**
```bash
curl -X POST http://localhost:3000/api/sequences \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-api-key" \
  -d '{
    "name": "Welcome Series",
    "description": "New customer welcome sequence",
    "steps": [
      {
        "stepNumber": 1,
        "subject": "Welcome {{contact.firstName}}!",
        "body": "Welcome to our platform, {{contact.firstName}}!",
        "delayHours": 0
      },
      {
        "stepNumber": 2,
        "subject": "Getting started guide",
        "body": "Here are some tips to get started...",
        "delayHours": 24
      }
    ]
  }'
```

**Enroll a Contact:**
```bash
curl -X POST http://localhost:3000/api/enrollments \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-api-key" \
  -d '{
    "sequenceId": "sequence-id-here",
    "contactEmail": "contact@example.com",
    "contactName": "John Doe"
  }'
```

## Worker Architecture

The service uses BullMQ for background job processing:

### Queues

1. **sequence-steps**: Process individual sequence steps
2. **warmup**: Mailbox warmup tasks
3. **watchdog**: Periodic monitoring tasks
4. **webhooks**: Webhook delivery with retry logic

### Workers

1. **Sequence Step Worker**: Sends emails, handles retries (30s, 90s, 270s)
2. **Signal Detection Worker**: Polls Gmail for replies, bounces, OOF (every 5min)
3. **OOF Resume Worker**: Checks if out-of-office mailboxes should resume (daily)
4. **Circuit Breaker Worker**: Monitors bounce rates >2% (hourly)
5. **Sent Today Reset Worker**: Resets daily send counters (midnight UTC)
6. **Webhook Delivery Worker**: Delivers webhooks with HMAC signing and exponential backoff

## Template System

Uses Handlebars for email personalization:

### Available Variables
```handlebars
{{contact.email}}        <!-- Recipient email -->
{{contact.name}}         <!-- Full name -->
{{contact.firstName}}    <!-- First name only -->
{{contact.lastName}}     <!-- Last name only -->

{{sender.email}}         <!-- Sender email -->
{{sender.name}}          <!-- Sender full name -->
{{sender.firstName}}     <!-- Sender first name -->
{{sender.lastName}}      <!-- Sender last name -->

{{sequence.name}}        <!-- Sequence name -->
{{step.number}}          <!-- Current step number -->
```

### Helpers
- `{{capitalize text}}`
- `{{upper text}}`
- `{{lower text}}`
- `{{default value "fallback"}}`
- `{{formatDate date "format"}}`
- Conditional helpers: `eq`, `ne`, `gt`, `lt`, `and`, `or`

## Gmail Integration

### Setup (Optional)

1. Enable Gmail API in Google Cloud Console
2. Create OAuth2 credentials
3. Set environment variables:
   ```bash
   GMAIL_ENABLED=true
   GOOGLE_CLIENT_ID=your-client-id
   GOOGLE_CLIENT_SECRET=your-client-secret
   GOOGLE_REDIRECT_URI=your-redirect-uri
   ```

### Features

- **Email Sending**: Send via Gmail API with proper threading
- **Signal Detection**: Detect replies, bounces, out-of-office responses
- **OAuth2 Flow**: Secure token management with automatic refresh
- **Stub Mode**: Test without Gmail API (`GMAIL_ENABLED=false`)

## Monitoring

### Health Check
```bash
curl http://localhost:3000/health
```

### Mailbox Statistics
```bash
curl -H "X-API-Key: your-api-key" http://localhost:3000/api/mailboxes
```

### Signal Analytics
```bash
curl -H "X-API-Key: your-api-key" http://localhost:3000/api/signals/stats
```

## Development

### Database Operations
```bash
npm run db:generate    # Generate Prisma client
npm run db:push        # Push schema to database
npm run db:migrate     # Create migration
npm run db:seed        # Seed test data
npm run db:studio      # Open Prisma Studio
```

### Docker Operations
```bash
npm run docker:up      # Start Postgres + Redis
npm run docker:down    # Stop containers
npm run docker:reset   # Reset with fresh data
```

### Code Quality
```bash
npm run lint           # Check linting
npm run lint:fix       # Fix linting issues
npm run format         # Format code
npm run format:check   # Check formatting
```

### Testing
```bash
npm test               # Run tests
npm run test:coverage  # Run with coverage
```

## Production Deployment

1. **Build the application:**
   ```bash
   npm run build
   ```

2. **Set production environment:**
   ```bash
   NODE_ENV=production
   DATABASE_URL=your-production-db-url
   REDIS_URL=your-production-redis-url
   ```

3. **Run migrations:**
   ```bash
   npm run db:migrate
   ```

4. **Start the service:**
   ```bash
   npm start
   ```

## Webhook Events

The service can send webhooks for these events:

- `email.sent` - Email successfully sent
- `email.bounced` - Email bounced
- `signal.detected` - Reply/bounce/OOF detected
- `circuit_breaker.triggered` - High bounce rate detected
- `enrollment.completed` - Sequence completed
- `enrollment.paused` - Enrollment paused
- `mailbox.daily_reset` - Daily send counters reset

Webhooks include HMAC-SHA256 signatures in the `X-Webhook-Signature` header.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Run `npm run lint` and `npm test`
6. Submit a pull request

## License

MIT License - see LICENSE file for details.