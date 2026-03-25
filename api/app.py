from fastapi import FastAPI, Request
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from api.limiter import limiter
from api.routes.demo import router as demo_router
from api.routes.private_sync import router as private_sync_router

tags_metadata = [
    {
        "name": "demo",
        "description": (
            "Public endpoints — no authentication required. "
            "PII fields (customer identity, register) are excluded from all responses."
        ),
    },
    {
        "name": "private",
        "description": (
            "Authenticated endpoints for incremental data sync. "
            "All requests must include a valid `X-API-Key` header. "
            "These routes are shown here for reference — a valid key is required to call them."
        ),
    },
]

app = FastAPI(
    title="Bakehouse Source Data API",
    version="0.1.0",
    description=(
        "Append-only REST API for a fictional bakehouse retailer, built as a realistic source system "
        "for a downstream data pipeline (ingestion → transformation → analytics).\n\n"
        "**Demo routes** (`/demo/*`) are public and freely usable — explore the data, no key needed.\n\n"
        "**Private routes** (`/v1/private/*`) power incremental keyset-paginated sync for ingestion jobs. "
        "They are shown here for reference and require an `X-API-Key` header to call."
    ),
    openapi_tags=tags_metadata,
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


@app.get("/health")
@limiter.limit("50/day")
def health_check(request: Request):
    return {"status": "ok"}


app.include_router(private_sync_router)
app.include_router(demo_router)
