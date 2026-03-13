-- Indexes to support API keyset pagination and incremental pulls
-- Apply once after schema creation:
--   psql "$DATABASE_URL" -f sql/api_indexes.sql

CREATE INDEX IF NOT EXISTS idx_transactions_sync_cursor
ON prod.transactions (transaction_datetime, transaction_id);

CREATE INDEX IF NOT EXISTS idx_items_sync_cursor
ON prod.transaction_items (transaction_id, line_number);
