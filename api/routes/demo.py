from fastapi import APIRouter, Query, Request
from sqlalchemy import text

from api.db import get_engine
from api.limiter import limiter
from api.schemas import DemoSummary, DemoTransaction, DemoTransactionPage

router = APIRouter(prefix="/demo", tags=["demo"])


@router.get(
    "/summary",
    response_model=DemoSummary,
    summary="Aggregated sales summary",
    description="Returns high-level sales metrics for the current data window. No authentication required. PII fields are excluded.",
    response_description="Aggregated summary of transactions in the current retention window",
)
@limiter.limit("50/day")
def demo_summary(request: Request):
    engine = get_engine()

    with engine.connect() as connection:
        summary = connection.execute(
            text(
                """
                SELECT
                    timezone('UTC', now()) AS watermark_utc,
                    MAX(transaction_date) AS latest_transaction_date,
                    COUNT(*)::BIGINT AS total_transactions,
                    COALESCE(SUM(total), 0) AS total_revenue
                FROM prod.transactions
                """
            )
        ).mappings().one()

        top_products = connection.execute(
            text(
                """
                SELECT
                    product_name,
                    SUM(quantity) AS total_quantity,
                    SUM(line_total) AS total_revenue
                FROM prod.transaction_items
                GROUP BY product_name
                ORDER BY total_quantity DESC
                LIMIT 5
                """
            )
        ).mappings().all()

        return DemoSummary(
            watermark_utc=summary["watermark_utc"],
            latest_transaction_date=summary["latest_transaction_date"],
            total_transactions=summary["total_transactions"],
            total_revenue=summary["total_revenue"],
            top_products=[dict(row) for row in top_products],
        )


@router.get(
    "/transactions",
    response_model=DemoTransactionPage,
    summary="Browse recent transactions",
    description="Returns a paginated list of recent transactions, ordered most-recent first. Limit is capped at 100. No authentication required. PII fields (customer identity, register) are excluded.",
    response_description="A page of sanitised transaction records",
)
@limiter.limit("50/day")
def demo_transactions(request: Request, limit: int = Query(default=25, ge=1, le=100)):
    engine = get_engine()

    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT
                    transaction_id::text AS transaction_id,
                    transaction_datetime,
                    store_id,
                    payment_method,
                    total
                FROM prod.transactions
                ORDER BY transaction_datetime DESC, transaction_id DESC
                LIMIT :limit
                """
            ),
            {"limit": limit},
        ).mappings().all()

        return DemoTransactionPage(data=[DemoTransaction(**row) for row in rows])
