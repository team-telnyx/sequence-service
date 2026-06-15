"""H4 (REVOPS-972): the API-key auth middleware must return a clean 401.

The auth check lived inside `@app.middleware("http")` and raised
`HTTPException(401)`. Starlette's BaseHTTPMiddleware does NOT translate an
HTTPException raised inside pure-http middleware into a response — it bubbles
up as an unhandled exception and the client receives HTTP 500. That made Scout
unable to distinguish an auth failure (don't retry) from a server error
(retry), risking retry storms.

These tests pin the contract: missing key -> 401, wrong key -> 401, correct
key -> 200 (never 500 on the auth path).
"""

import pytest

# A protected endpoint (auth middleware applies; not /health or /track/*).
PROTECTED_PATH = "/api/mailboxes/"


@pytest.mark.asyncio
async def test_missing_api_key_returns_401_not_500(client, seeded):
    resp = await client.get(PROTECTED_PATH)  # no X-API-Key header
    assert resp.status_code == 401
    assert resp.json()["detail"] == "Missing API key"


@pytest.mark.asyncio
async def test_wrong_api_key_returns_401_not_500(client, seeded):
    resp = await client.get(PROTECTED_PATH, headers={"X-API-Key": "totally-wrong-key"})
    assert resp.status_code == 401
    assert resp.json()["detail"] == "Invalid API key"


@pytest.mark.asyncio
async def test_correct_api_key_returns_200(client, seeded):
    resp = await client.get(
        PROTECTED_PATH, headers={"X-API-Key": seeded["api_key"]}
    )
    assert resp.status_code == 200
