-- Bakehouse core schema for customers, transactions, and transaction_items
-- Assumes standard PostgreSQL (e.g., Railway). No extensions required.

-- ============================================================================
-- Database and Schema Setup
-- ============================================================================
-- Run this block ONCE to initialize the database and schemas on Railway.
-- After this, the schema creation can be commented out for subsequent runs.

-- psql $RAILWAY_DATABASE_URL -c "CREATE DATABASE IF NOT EXISTS bakehouse_dev;"

-- Connect to bakehouse_dev and run the schema creation below:
-- psql $RAILWAY_DATABASE_URL -c "CREATE SCHEMA IF NOT EXISTS stg;"
-- psql $RAILWAY_DATABASE_URL -c "CREATE SCHEMA IF NOT EXISTS prod;"
-- psql $RAILWAY_DATABASE_URL -c "CREATE SCHEMA IF NOT EXISTS analytics;"

-- ============================================================================
-- PRODUCTION SCHEMA: Core tables for customers, transactions, line items
-- ============================================================================

-- Customers: master data for customer attributes used in analytics
CREATE TABLE IF NOT EXISTS prod.customers (
    customer_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- If gen_random_uuid() is unavailable, replace with uuid_generate_v4() and enable uuid-ossp
    email TEXT UNIQUE,
    first_name TEXT,
    last_name TEXT,
    phone TEXT,
    loyalty_status TEXT NOT NULL, -- e.g., 'none', 'member', 'vip'
    loyalty_joined_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Transactions: header-level data for each sale
CREATE TABLE IF NOT EXISTS prod.transactions (
    transaction_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id UUID NULL,
    transaction_datetime TIMESTAMPTZ NOT NULL, -- precise timestamp of sale
    transaction_date DATE NOT NULL, -- used for retention policy and partitioning
    store_id TEXT,
    register_id TEXT,
    payment_method TEXT, -- e.g., 'cash', 'card', 'mobile'
    subtotal NUMERIC(12,2) NOT NULL,
    tax NUMERIC(12,2) NOT NULL DEFAULT 0,
    total NUMERIC(12,2) NOT NULL,
    currency_code CHAR(3) NOT NULL DEFAULT 'USD',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_transactions_customer
        FOREIGN KEY (customer_id) REFERENCES prod.customers(customer_id)
        ON UPDATE NO ACTION ON DELETE SET NULL
);

-- Transaction line items: detail rows per product in a transaction
CREATE TABLE IF NOT EXISTS prod.transaction_items (
    transaction_id UUID NOT NULL,
    line_number INTEGER NOT NULL, -- sequential per transaction for deterministic ordering
    product_id TEXT NOT NULL, -- future FK to a products table
    product_name TEXT, -- optional denormalized label for ease of analysis
    quantity NUMERIC(12,3) NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0),
    discount_amount NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (discount_amount >= 0),
    line_total NUMERIC(12,2) GENERATED ALWAYS AS ((quantity * unit_price) - discount_amount) STORED,
    -- Optional cost for COGS; if not provided at ingestion, can be backfilled
    unit_cost NUMERIC(12,4),
    line_cogs NUMERIC(12,4) GENERATED ALWAYS AS (CASE WHEN unit_cost IS NOT NULL THEN quantity * unit_cost ELSE NULL END) STORED,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (transaction_id, line_number),
    CONSTRAINT fk_items_transaction
        FOREIGN KEY (transaction_id) REFERENCES prod.transactions(transaction_id)
        ON UPDATE NO ACTION ON DELETE CASCADE
);

-- ============================================================================
-- Indexes for high-volume analytical queries
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_transactions_customer ON prod.transactions(customer_id);
CREATE INDEX IF NOT EXISTS idx_transactions_date ON prod.transactions(transaction_date);
CREATE INDEX IF NOT EXISTS idx_items_product ON prod.transaction_items(product_id);
CREATE INDEX IF NOT EXISTS idx_items_transaction ON prod.transaction_items(transaction_id);

-- ============================================================================
-- Triggers to maintain updated_at timestamps and derive transaction_date
-- ============================================================================

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at := NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION set_transaction_date()
RETURNS TRIGGER AS $$
BEGIN
    NEW.transaction_date := NEW.transaction_datetime::date;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DO $$
BEGIN
    -- Customers updated_at trigger
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 't_customers_set_updated_at') THEN
        CREATE TRIGGER t_customers_set_updated_at
        BEFORE UPDATE ON prod.customers
        FOR EACH ROW EXECUTE FUNCTION set_updated_at();
    END IF;

    -- Transactions updated_at trigger
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 't_transactions_set_updated_at') THEN
        CREATE TRIGGER t_transactions_set_updated_at
        BEFORE UPDATE ON prod.transactions
        FOR EACH ROW EXECUTE FUNCTION set_updated_at();
    END IF;

    -- Transactions transaction_date trigger (derive from transaction_datetime)
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 't_transactions_set_date') THEN
        CREATE TRIGGER t_transactions_set_date
        BEFORE INSERT OR UPDATE OF transaction_datetime ON prod.transactions
        FOR EACH ROW EXECUTE FUNCTION set_transaction_date();
    END IF;

    -- Transaction items updated_at trigger
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 't_items_set_updated_at') THEN
        CREATE TRIGGER t_items_set_updated_at
        BEFORE UPDATE ON prod.transaction_items
        FOR EACH ROW EXECUTE FUNCTION set_updated_at();
    END IF;
END;
$$;