"""FastAPI application for Sequence Service."""

from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

import logging

from src.config import get_settings
from src.models.base import engine, Base, async_session
from src.api import (
    enrollments,
    sequences,
    mailboxes,
    webhooks,
    suppressions,
    email_events,
    v1_enrollments,
)

settings = get_settings()
logger = logging.getLogger("sequence_service")


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    await engine.dispose()


app = FastAPI(
    title="Sequence Service",
    description="Internal email sequencing service for Telnyx AI products",
    version="0.1.0",
    lifespan=lifespan,
)

# CORS — explicit origins only; wildcard+credentials is a CSRF surface (not_done_if).
# Empty list = no CORS (fail-closed). CORS_ALLOWED_ORIGINS env var.
#
# SV2-044 r3 (not_done_if): the r2 builder constructed
# ``CORSMiddleware(allow_origins=['*'], allow_credentials=True)`` when
# ``CORS_ALLOWED_ORIGINS='["*"]'`` — wildcard origins + credentials is the
# classic CSRF surface (browsers refuse it per the CORS spec, but
# starlette's CORSMiddleware does NOT refuse it server-side — it emits the
# ``Access-Control-Allow-Origin: *`` header with
# ``Access-Control-Allow-Credentials: true``, which is a spec violation
# AND a real CSRF surface if a future client coerces the response). The r3
# guard makes the dangerous combination IMPOSSIBLE by construction:
#   - if any configured origin is "*" → raise on invalid config (refuse to
#     start). This is the fail-closed path — there is NO configuration that
#     yields wildcard+credentials.
# An operator who genuinely wants wildcard (no credentials) must explicitly
# set ``allow_credentials=False`` via a future config knob; the default
# remains "explicit origins only" so the dangerous combo cannot land by
# accident.
_cors_origins = list(settings.cors_allowed_origins)
if _cors_origins:
    # not_done_if: reject "*" — wildcard origins + credentials is a CSRF
    # surface. Refuse to start so misconfiguration is loud, not silent.
    if any(origin == "*" for origin in _cors_origins):
        raise RuntimeError(
            "CORS_ALLOWED_ORIGINS contains '*' — wildcard origins with "
            "allow_credentials=True is a CSRF surface (not_done_if). "
            "Configure explicit origins only."
        )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=_cors_origins,
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE"],
        allow_headers=["X-API-Key", "Content-Type", "Authorization"],
    )
else:
    logger.info("CORS disabled — CORS_ALLOWED_ORIGINS empty (fail-closed).")


# Paths that skip X-API-Key auth: health (unauthenticated by spec) and the
# Email API webhook (Ed25519 raw-body verify replaces tenant auth).
_NO_AUTH_PATHS = frozenset({"/health", "/health/live", "/health/ready"})


def _is_health_or_webhook(path: str) -> bool:
    return path in _NO_AUTH_PATHS or path == "/webhooks/email-events"


@app.middleware("http")
async def authenticate_tenant(request: Request, call_next):
    """Validate X-API-Key against SEQUENCE_SERVICE_API_KEY env var (not the
    vestigial DB tenants.api_key column, which 401s in production).

    H4: returns JSONResponse(401) directly — raising HTTPException in
    BaseHTTPMiddleware surfaces as 500, causing retry storms.
    """
    if _is_health_or_webhook(request.url.path):
        return await call_next(request)

    api_key = request.headers.get("X-API-Key")
    if not api_key:
        return JSONResponse(status_code=401, content={"detail": "Missing API key"})

    expected_key = settings.sequence_service_api_key
    if not expected_key:
        logger.error("SEQUENCE_SERVICE_API_KEY not configured — rejecting (fail closed).")
        return JSONResponse(status_code=503, content={"detail": "Service not configured for auth"})

    # Constant-time compare — avoids timing side channels.
    import hmac as _hmac

    if not _hmac.compare_digest(api_key, expected_key):
        return JSONResponse(status_code=401, content={"detail": "Invalid API key"})

    # Scout-only deployment: single authenticated tenant is tenant-scout.
    request.state.tenant_id = "tenant-scout"

    return await call_next(request)


@app.get("/health")
async def health_check():
    return {"status": "healthy", "version": "0.1.0", "service": "sequence-service"}


@app.get("/health/live")
async def health_live():
    return {"status": "alive"}


@app.get("/health/ready")
async def health_ready():
    return {"status": "ready"}


app.include_router(sequences.router, prefix="/api/sequences", tags=["sequences"])
app.include_router(enrollments.router, prefix="/api/enrollments", tags=["enrollments"])
app.include_router(mailboxes.router, prefix="/api/mailboxes", tags=["mailboxes"])
app.include_router(webhooks.router, prefix="/api/webhooks", tags=["webhooks"])
app.include_router(suppressions.router, prefix="/api/suppressions", tags=["suppressions"])
app.include_router(email_events.router, prefix="/webhooks", tags=["email-events"])
app.include_router(v1_enrollments.router, prefix="/v1", tags=["v1-contracts"])


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=settings.api_host, port=settings.api_port)
