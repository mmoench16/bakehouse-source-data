from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional

from pydantic import BaseModel, Field


class ApiMetadata(BaseModel):
    watermark_utc: datetime
    retention_cutoff_date: date
    append_only_mode: bool = True
    cdc_future_stub: str = "Phase 2: updated_at watermark + merge/dedupe policy"


class TransactionRecord(BaseModel):
    transaction_id: str
    customer_id: Optional[str]
    transaction_datetime: datetime
    transaction_date: date
    store_id: Optional[str]
    register_id: Optional[str]
    payment_method: Optional[str]
    subtotal: Decimal
    tax: Decimal
    total: Decimal
    currency_code: str
    idempotency_key: str


class TransactionItemRecord(BaseModel):
    transaction_id: str
    line_number: int
    product_id: str
    product_name: Optional[str]
    quantity: Decimal
    unit_price: Decimal
    discount_amount: Decimal
    line_total: Decimal
    unit_cost: Optional[Decimal]
    line_cogs: Optional[Decimal]
    transaction_datetime: datetime
    idempotency_key: str


class CursorPage(BaseModel):
    next_cursor: Optional[str]
    has_more: bool


class TransactionPage(BaseModel):
    metadata: ApiMetadata
    paging: CursorPage
    data: List[TransactionRecord]


class TransactionItemPage(BaseModel):
    metadata: ApiMetadata
    paging: CursorPage
    data: List[TransactionItemRecord]


class DemoSummary(BaseModel):
    watermark_utc: datetime = Field(description="Time this summary was generated (UTC)")
    latest_transaction_date: Optional[date] = Field(description="Date of the most recent transaction in the dataset")
    total_transactions: int = Field(description="Total number of transactions in the current retention window")
    total_revenue: Decimal = Field(description="Sum of all transaction totals in GBP")
    top_products: list[dict] = Field(description="Top 5 products by quantity sold, each with product_name, total_quantity, and total_revenue")


class DemoTransaction(BaseModel):
    transaction_id: str = Field(description="Unique identifier for the transaction")
    transaction_datetime: datetime = Field(description="UTC timestamp of the transaction")
    store_id: Optional[str] = Field(description="Store location identifier")
    payment_method: Optional[str] = Field(description="Payment method used (e.g. card, cash)")
    total: Decimal = Field(description="Transaction total in GBP")


class DemoTransactionPage(BaseModel):
    data: list[DemoTransaction]
    note: str = Field(default="PII fields are intentionally excluded from demo responses")
