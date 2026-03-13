import base64
import json
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text

from api.auth import verify_api_key
from api.db import get_engine
from api.schemas import (
    ApiMetadata,
    CursorPage,
    TransactionItemPage,
    TransactionItemRecord,
    TransactionPage,
    TransactionRecord,
)

router = APIRouter(prefix="/v1/private", tags=["private"], dependencies=[Depends(verify_api_key)])


def encode_cursor(payload: dict) -> str:
    json_payload = json.dumps(payload, separators=(",", ":"))
    return base64.urlsafe_b64encode(json_payload.encode("utf-8")).decode("utf-8")


def decode_cursor(cursor: str) -> dict:
    try:
        decoded = base64.urlsafe_b64decode(cursor.encode("utf-8")).decode("utf-8")
        return json.loads(decoded)
    except Exception as error:
        raise HTTPException(status_code=400, detail=f"Invalid cursor: {error}") from error


def get_metadata(connection) -> ApiMetadata:
    retention_cutoff_date = connection.execute(
        text(
            """
            SELECT (
                date_trunc('month', timezone('UTC', now()))::date
                - make_interval(months => 2)
            )::date
            """
        )
    ).scalar_one()

    return ApiMetadata(
        watermark_utc=datetime.now(timezone.utc),
        retention_cutoff_date=retention_cutoff_date,
    )


@router.get("/transactions", response_model=TransactionPage)
def get_transactions(
    limit: int = Query(default=500, ge=1, le=5000),
    after: Optional[str] = Query(default=None),
):
    engine = get_engine()

    with engine.connect() as connection:
        if after:
            cursor_payload = decode_cursor(after)
            last_datetime = cursor_payload.get("transaction_datetime")
            last_transaction_id = cursor_payload.get("transaction_id")
            if not last_datetime or not last_transaction_id:
                raise HTTPException(
                    status_code=400,
                    detail="Cursor for /transactions must include transaction_datetime and transaction_id",
                )

            rows = connection.execute(
                text(
                    """
                    SELECT
                        transaction_id::text AS transaction_id,
                        customer_id::text AS customer_id,
                        transaction_datetime,
                        transaction_date,
                        store_id,
                        register_id,
                        payment_method,
                        subtotal,
                        tax,
                        total,
                        currency_code
                    FROM prod.transactions
                    WHERE (
                        transaction_datetime > CAST(:last_datetime AS TIMESTAMPTZ)
                        OR (
                            transaction_datetime = CAST(:last_datetime AS TIMESTAMPTZ)
                            AND transaction_id > CAST(:last_transaction_id AS UUID)
                        )
                    )
                    ORDER BY transaction_datetime, transaction_id
                    LIMIT :limit_plus_one
                    """
                ),
                {
                    "last_datetime": last_datetime,
                    "last_transaction_id": last_transaction_id,
                    "limit_plus_one": limit + 1,
                },
            ).mappings().all()
        else:
            rows = connection.execute(
                text(
                    """
                    SELECT
                        transaction_id::text AS transaction_id,
                        customer_id::text AS customer_id,
                        transaction_datetime,
                        transaction_date,
                        store_id,
                        register_id,
                        payment_method,
                        subtotal,
                        tax,
                        total,
                        currency_code
                    FROM prod.transactions
                    ORDER BY transaction_datetime, transaction_id
                    LIMIT :limit_plus_one
                    """
                ),
                {"limit_plus_one": limit + 1},
            ).mappings().all()

        has_more = len(rows) > limit
        page_rows = rows[:limit]

        data = [
            TransactionRecord(
                transaction_id=row["transaction_id"],
                customer_id=row["customer_id"],
                transaction_datetime=row["transaction_datetime"],
                transaction_date=row["transaction_date"],
                store_id=row["store_id"],
                register_id=row["register_id"],
                payment_method=row["payment_method"],
                subtotal=row["subtotal"],
                tax=row["tax"],
                total=row["total"],
                currency_code=row["currency_code"],
                idempotency_key=row["transaction_id"],
            )
            for row in page_rows
        ]

        next_cursor = None
        if page_rows and has_more:
            last_row = page_rows[-1]
            next_cursor = encode_cursor(
                {
                    "transaction_datetime": last_row["transaction_datetime"].isoformat(),
                    "transaction_id": last_row["transaction_id"],
                }
            )

        return TransactionPage(
            metadata=get_metadata(connection),
            paging=CursorPage(next_cursor=next_cursor, has_more=has_more),
            data=data,
        )


@router.get("/transaction-items", response_model=TransactionItemPage)
def get_transaction_items(
    limit: int = Query(default=1000, ge=1, le=10000),
    after: Optional[str] = Query(default=None),
):
    engine = get_engine()

    with engine.connect() as connection:
        if after:
            cursor_payload = decode_cursor(after)
            last_datetime = cursor_payload.get("transaction_datetime")
            last_transaction_id = cursor_payload.get("transaction_id")
            last_line_number = cursor_payload.get("line_number")
            if not last_datetime or not last_transaction_id or last_line_number is None:
                raise HTTPException(
                    status_code=400,
                    detail="Cursor for /transaction-items must include transaction_datetime, transaction_id, and line_number",
                )

            rows = connection.execute(
                text(
                    """
                    SELECT
                        ti.transaction_id::text AS transaction_id,
                        ti.line_number,
                        ti.product_id,
                        ti.product_name,
                        ti.quantity,
                        ti.unit_price,
                        ti.discount_amount,
                        ti.line_total,
                        ti.unit_cost,
                        ti.line_cogs,
                        t.transaction_datetime
                    FROM prod.transaction_items ti
                    JOIN prod.transactions t
                        ON t.transaction_id = ti.transaction_id
                    WHERE (
                        t.transaction_datetime > CAST(:last_datetime AS TIMESTAMPTZ)
                        OR (
                            t.transaction_datetime = CAST(:last_datetime AS TIMESTAMPTZ)
                            AND (
                                ti.transaction_id > CAST(:last_transaction_id AS UUID)
                                OR (
                                    ti.transaction_id = CAST(:last_transaction_id AS UUID)
                                    AND ti.line_number > CAST(:last_line_number AS INTEGER)
                                )
                            )
                        )
                    )
                    ORDER BY t.transaction_datetime, ti.transaction_id, ti.line_number
                    LIMIT :limit_plus_one
                    """
                ),
                {
                    "last_datetime": last_datetime,
                    "last_transaction_id": last_transaction_id,
                    "last_line_number": last_line_number,
                    "limit_plus_one": limit + 1,
                },
            ).mappings().all()
        else:
            rows = connection.execute(
                text(
                    """
                    SELECT
                        ti.transaction_id::text AS transaction_id,
                        ti.line_number,
                        ti.product_id,
                        ti.product_name,
                        ti.quantity,
                        ti.unit_price,
                        ti.discount_amount,
                        ti.line_total,
                        ti.unit_cost,
                        ti.line_cogs,
                        t.transaction_datetime
                    FROM prod.transaction_items ti
                    JOIN prod.transactions t
                        ON t.transaction_id = ti.transaction_id
                    ORDER BY t.transaction_datetime, ti.transaction_id, ti.line_number
                    LIMIT :limit_plus_one
                    """
                ),
                {"limit_plus_one": limit + 1},
            ).mappings().all()

        has_more = len(rows) > limit
        page_rows = rows[:limit]

        data = [
            TransactionItemRecord(
                transaction_id=row["transaction_id"],
                line_number=row["line_number"],
                product_id=row["product_id"],
                product_name=row["product_name"],
                quantity=row["quantity"],
                unit_price=row["unit_price"],
                discount_amount=row["discount_amount"],
                line_total=row["line_total"],
                unit_cost=row["unit_cost"],
                line_cogs=row["line_cogs"],
                transaction_datetime=row["transaction_datetime"],
                idempotency_key=f"{row['transaction_id']}:{row['line_number']}",
            )
            for row in page_rows
        ]

        next_cursor = None
        if page_rows and has_more:
            last_row = page_rows[-1]
            next_cursor = encode_cursor(
                {
                    "transaction_datetime": last_row["transaction_datetime"].isoformat(),
                    "transaction_id": last_row["transaction_id"],
                    "line_number": last_row["line_number"],
                }
            )

        return TransactionItemPage(
            metadata=get_metadata(connection),
            paging=CursorPage(next_cursor=next_cursor, has_more=has_more),
            data=data,
        )
