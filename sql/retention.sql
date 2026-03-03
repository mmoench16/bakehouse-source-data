-- Monthly retention policy for Bakehouse transactional data
-- Keeps only the most recent N calendar months (default: 3), including current month.
--
-- Example (run once after schema setup):
--   psql "$DATABASE_URL" -f sql/retention.sql
--
-- Example (manual execution):
--   CALL prod.apply_data_retention(3);

CREATE OR REPLACE PROCEDURE prod.apply_data_retention(months_to_keep INTEGER DEFAULT 3)
LANGUAGE plpgsql
AS $$
DECLARE
    cutoff_date DATE;
    deleted_transactions BIGINT := 0;
    deleted_customers BIGINT := 0;
BEGIN
    IF months_to_keep < 1 THEN
        RAISE EXCEPTION 'months_to_keep must be >= 1';
    END IF;

    cutoff_date := (
        date_trunc('month', timezone('UTC', now()))::date
        - make_interval(months => months_to_keep - 1)
    )::date;

    DELETE FROM prod.transactions
    WHERE transaction_date < cutoff_date;

    GET DIAGNOSTICS deleted_transactions = ROW_COUNT;

    -- Optional business rule from project setup:
    -- Remove customers that no longer have any transactions after retention.
    DELETE FROM prod.customers c
    WHERE NOT EXISTS (
        SELECT 1
        FROM prod.transactions t
        WHERE t.customer_id = c.customer_id
    );

    GET DIAGNOSTICS deleted_customers = ROW_COUNT;

    RAISE NOTICE 'Retention cutoff date: %', cutoff_date;
    RAISE NOTICE 'Deleted transactions: %', deleted_transactions;
    RAISE NOTICE 'Deleted orphan customers: %', deleted_customers;
END;
$$;
