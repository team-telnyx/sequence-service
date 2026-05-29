"""FastAPI application for Sequence Service."""

from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.ext.asyncio import AsyncSession

import logging
import socket
from urllib.parse import urlparse

from src.config import get_settings
from src.models.base import get_db, engine, Base, async_session
from src.api import enrollments, sequences, mailboxes, webhooks, tracking, suppressions

settings = get_settings()
logger = logging.getLogger("sequence_service")


def _tracking_host_reachable() -> bool:
    """DNS-resolve the tracking_base_url host (Wave 0 health probe) so a dead
    unsubscribe/tracking host (e.g. track.telnyx.com NXDOMAIN) can never silently
    ship on every email again."""
    try:
        host = urlparse(settings.tracking_base_url).hostname
        if not host:
            return False
        socket.getaddrinfo(host, None)
        return True
    except Exception:
        return False


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    # Startup: create tables if they don't exist (dev only)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Wave 0 health probe: if open/click tracking OR one-click unsubscribe is on,
    # the tracking host MUST resolve — else every email ships a dead link/endpoint.
    # Fail LOUDLY rather than silently shipping a dead host (the original bug).
    if settings.tracking_enabled or settings.one_click_unsubscribe_enabled:
        if not _tracking_host_reachable():
            logger.critical(
                "TRACKING HOST UNREACHABLE: %s does not resolve, but tracking_enabled=%s / "
                "one_click_unsubscribe_enabled=%s — emails would ship dead tracking/unsubscribe "
                "links. Set a reachable TRACKING_BASE_URL or disable these flags.",
                settings.tracking_base_url, settings.tracking_enabled,
                settings.one_click_unsubscribe_enabled,
            )
        else:
            logger.info("Tracking host reachable: %s", settings.tracking_base_url)
    else:
        logger.info(
            "Tracking + one-click unsubscribe disabled; using mailto unsubscribe "
            "(reachable TRACKING_BASE_URL not required).",
        )
    yield
    # Shutdown
    await engine.dispose()


app = FastAPI(
    title="Sequence Service",
    description="Internal email sequencing service for Telnyx AI products",
    version="0.1.0",
    lifespan=lifespan,
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Tenant authentication middleware
@app.middleware("http")
async def authenticate_tenant(request: Request, call_next):
    """Extract and validate tenant from API key."""
    # Skip auth for health check and tracking endpoints
    if request.url.path == "/health" or request.url.path.startswith("/track/"):
        return await call_next(request)
    
    api_key = request.headers.get("X-API-Key")
    if not api_key:
        raise HTTPException(status_code=401, detail="Missing API key")
    
    # Look up tenant by API key
    from sqlalchemy import select
    from src.models.models import Tenant
    
    async with async_session() as db:
        result = await db.execute(
            select(Tenant).where(Tenant.api_key == api_key)
        )
        tenant = result.scalar_one_or_none()
    
    if not tenant:
        raise HTTPException(status_code=401, detail="Invalid API key")
    
    request.state.tenant_id = tenant.id
    
    return await call_next(request)


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "version": "0.1.0",
        "service": "sequence-service",
    }


# Register routers
app.include_router(sequences.router, prefix="/api/sequences", tags=["sequences"])
app.include_router(enrollments.router, prefix="/api/enrollments", tags=["enrollments"])
app.include_router(mailboxes.router, prefix="/api/mailboxes", tags=["mailboxes"])
app.include_router(webhooks.router, prefix="/api/webhooks", tags=["webhooks"])
app.include_router(tracking.router, prefix="/track", tags=["tracking"])
app.include_router(suppressions.router, prefix="/api/suppressions", tags=["suppressions"])


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=settings.api_host, port=settings.api_port)
