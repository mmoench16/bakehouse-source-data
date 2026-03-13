from fastapi import FastAPI

from api.routes.demo import router as demo_router
from api.routes.private_sync import router as private_sync_router

app = FastAPI(
    title="Bakehouse Source Data API",
    version="0.1.0",
    description=(
        "Append-only API for sync into downstream platforms. "
        "Private routes require API key; demo routes are sanitized."
    ),
)


@app.get("/health")
def health_check():
    return {"status": "ok"}


app.include_router(private_sync_router)
app.include_router(demo_router)
