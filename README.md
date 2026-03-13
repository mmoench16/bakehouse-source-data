# bakehouse-source-data

Generation of source data for the Bakehouse pipeline, plus a secure append-only API for downstream ingestion.

Detailed API documentation is available in `API_GUIDE.md`.

## Data Platform Components

- Monthly data generation script: `scripts/generate_monthly_data.py`
- Automated retention script (3-month policy): `scripts/run_retention.py`
- API service:
  - Private sync endpoints: `/v1/private/*` (API key required)
  - Public demo endpoints: `/demo/*` (sanitized, no PII)

## API Security Model

- Private endpoints require `X-API-Key` header matching `API_PRIVATE_KEY`.
- Demo endpoints expose only non-PII fields for portfolio demonstration.
- Current ingestion model is append-only; no upsert/update API semantics.

## Quick Start (API)

1. Install dependencies:

	```bash
	pip install -r scripts/requirements.txt
	```

2. Configure environment:

	```bash
	cp .env.example .env
	```

	Set `DATABASE_URL` and `API_PRIVATE_KEY`.

3. (Recommended) Apply API indexes:

	```bash
	psql "$DATABASE_URL" -f sql/api_indexes.sql
	```

4. Run the API:

	```bash
	uvicorn api.app:app --host 0.0.0.0 --port 8000
	```

## Endpoint Summary

- `GET /health`
- `GET /v1/private/transactions?limit=500&after=<cursor>`
- `GET /v1/private/transaction-items?limit=1000&after=<cursor>`
- `GET /demo/summary`
- `GET /demo/transactions?limit=25`

## Future CDC Direction (Planned)

The API intentionally stays append-only for now. For Phase 2, CDC scaffolding is prepared in `sql/cdc_stub.sql` for:

- `updated_at` watermark strategy
- late-arriving change handling
- idempotent merge/dedupe rules in downstream ingestion
