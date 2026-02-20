# Deployment Guide

## Phase 1 Implementation Status

✅ **Track A - Core Service Scaffold**: COMPLETE  
✅ **Track B - BullMQ Workflows**: COMPLETE  
✅ **Track C - Gmail Integration Scaffold**: COMPLETE  

All tracks implemented as an integrated system with full functionality.

## Quick Deploy

1. **Environment Setup**:
   ```bash
   cp .env.example .env
   # Configure DATABASE_URL, REDIS_URL, and other settings
   ```

2. **Database**:
   ```bash
   npm run db:push
   npm run db:seed
   ```

3. **Start Services**:
   ```bash
   npm run docker:up  # Start Postgres + Redis
   npm run build      # Build TypeScript
   npm start          # Start production server
   ```

## Production Checklist

- [ ] Set `NODE_ENV=production`
- [ ] Configure production `DATABASE_URL` and `REDIS_URL`
- [ ] Set up monitoring for workers and queues
- [ ] Configure webhook endpoints for your application
- [ ] Set up log aggregation
- [ ] Configure Gmail OAuth2 if using `GMAIL_ENABLED=true`
- [ ] Set up alerts for circuit breaker triggers
- [ ] Configure backup strategy for PostgreSQL

## Worker Monitoring

Monitor these BullMQ queues:
- `sequence-steps`: Email sending pipeline
- `watchdog`: Signal detection, OOF resume, circuit breaker
- `webhooks`: Webhook delivery with retries
- `warmup`: Mailbox warmup (future use)

## Security Notes

- API keys are UUIDs - rotate regularly
- Webhook secrets are HMAC-SHA256 signed
- Gmail OAuth tokens are automatically refreshed
- All tenant data is isolated by tenantId

## Scaling

- **Horizontal**: Add more worker instances
- **Vertical**: Increase `WORKER_CONCURRENCY` 
- **Database**: Use read replicas for analytics queries
- **Redis**: Use Redis Cluster for high availability