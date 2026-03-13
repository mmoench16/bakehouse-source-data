from fastapi import APIRouter, Query
from sqlalchemy import text

from api.db import get_engine
from api.schemas import DemoSummary, DemoTransaction, DemoTransactionPage

router = APIRouter(prefix="/demo", tags=["demo"])


@router.get("/summary", response_model=DemoSummary)
def demo_summary():
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


@router.get("/transactions", response_model=DemoTransactionPage)
def demo_transactions(limit: int = Query(default=25, ge=1, le=100)):
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
