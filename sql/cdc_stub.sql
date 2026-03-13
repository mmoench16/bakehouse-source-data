-- CDC future stub (Phase 2)
-- Current API is append-only and does not use this table directly yet.
-- This scaffold is to avoid future contract-breaking changes when
-- moving to updated_at watermark + late-arrival handling + merge/dedupe policies.
--
-- Apply when you want to start capturing CDC metadata:
--   psql "$DATABASE_URL" -f sql/cdc_stub.sql

CREATE SCHEMA IF NOT EXISTS api;

CREATE TABLE IF NOT EXISTS api.change_log_stub (
    event_id BIGSERIAL PRIMARY KEY,
    entity_name TEXT NOT NULL,
    entity_pk TEXT NOT NULL,
    operation TEXT NOT NULL CHECK (operation IN ('insert', 'update', 'delete')),
    emitted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_note TEXT NOT NULL DEFAULT 'stub_only'
);

CREATE INDEX IF NOT EXISTS idx_change_log_stub_emitted
ON api.change_log_stub (emitted_at, event_id);

-- Cursor state table for downstream consumers (e.g., Databricks jobs)
CREATE TABLE IF NOT EXISTS api.consumer_cursor_state (
    consumer_name TEXT PRIMARY KEY,
    cursor_value TEXT,
    watermark_utc TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
