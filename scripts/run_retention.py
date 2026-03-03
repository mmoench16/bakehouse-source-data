"""
Bakehouse Data Retention Runner
Applies an automated retention policy to keep only recent months of data.

Usage:
    python scripts/run_retention.py
    python scripts/run_retention.py --months-to-keep 3
    python scripts/run_retention.py --months-to-keep 3 --dry-run
"""

import argparse
import os
import sys

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Load environment variables from .env file
load_dotenv()


def get_database_url() -> str:
    """Get and normalize DATABASE_URL for SQLAlchemy + psycopg3."""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL environment variable not set")

    if database_url.startswith("postgresql://"):
        database_url = database_url.replace("postgresql://", "postgresql+psycopg://", 1)

    return database_url


def get_cutoff_date(connection, months_to_keep: int):
    """Compute retention cutoff date using UTC month boundaries in PostgreSQL."""
    query = text(
        """
        SELECT (
            date_trunc('month', timezone('UTC', now()))::date
            - make_interval(months => :months_back)
        )::date AS cutoff_date
        """
    )
    months_back = months_to_keep - 1
    result = connection.execute(query, {"months_back": months_back}).scalar_one()
    return result


def preview_deletions(connection, cutoff_date):
    """Return counts that would be removed by retention."""
    transactions_to_delete = connection.execute(
        text(
            """
            SELECT COUNT(*)
            FROM prod.transactions
            WHERE transaction_date < :cutoff_date
            """
        ),
        {"cutoff_date": cutoff_date},
    ).scalar_one()

    items_to_delete = connection.execute(
        text(
            """
            SELECT COUNT(*)
            FROM prod.transaction_items ti
            JOIN prod.transactions t ON t.transaction_id = ti.transaction_id
            WHERE t.transaction_date < :cutoff_date
            """
        ),
        {"cutoff_date": cutoff_date},
    ).scalar_one()

    orphan_customers = connection.execute(
        text(
            """
            SELECT COUNT(*)
            FROM prod.customers c
            WHERE NOT EXISTS (
                SELECT 1
                FROM prod.transactions t
                WHERE t.customer_id = c.customer_id
            )
            """
        )
    ).scalar_one()

    return transactions_to_delete, items_to_delete, orphan_customers


def run_retention(months_to_keep: int, dry_run: bool = False):
    """Execute or preview monthly retention."""
    database_url = get_database_url()
    engine = create_engine(database_url)

    with engine.begin() as connection:
        cutoff_date = get_cutoff_date(connection, months_to_keep)

        transactions_to_delete, items_to_delete, orphan_customers = preview_deletions(
            connection, cutoff_date
        )

        print("=" * 70)
        print("Bakehouse Data Retention")
        print("=" * 70)
        print(f"Months to keep: {months_to_keep}")
        print(f"Cutoff date (UTC month boundary): {cutoff_date}")
        print("Delete condition: transaction_date < cutoff_date")
        print("=" * 70)
        print(f"Transactions to delete: {transactions_to_delete:,}")
        print(f"Transaction items to delete (via cascade): {items_to_delete:,}")
        print(f"Orphan customers to delete after cleanup: {orphan_customers:,}")

        if dry_run:
            print("\nDry run complete. No data was deleted.")
            print("=" * 70)
            return

        connection.execute(
            text("CALL prod.apply_data_retention(CAST(:months_to_keep AS INTEGER))"),
            {"months_to_keep": months_to_keep},
        )

        remaining_date_range = connection.execute(
            text(
                """
                SELECT MIN(transaction_date), MAX(transaction_date)
                FROM prod.transactions
                """
            )
        ).one()

        print("\nRetention applied successfully.")
        print(
            f"Remaining transaction date range: {remaining_date_range[0]} to {remaining_date_range[1]}"
        )
        print("=" * 70)


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Run Bakehouse monthly data retention")
    parser.add_argument(
        "--months-to-keep",
        type=int,
        default=3,
        help="Number of most recent calendar months to keep (default: 3)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be deleted without applying changes",
    )
    args = parser.parse_args()

    if args.months_to_keep < 1:
        print("Error: --months-to-keep must be at least 1")
        sys.exit(1)

    try:
        run_retention(months_to_keep=args.months_to_keep, dry_run=args.dry_run)
    except Exception as error:
        print(f"Error: {error}")
        print("Hint: Ensure sql/retention.sql has been applied and DATABASE_URL is correct")
        sys.exit(1)


if __name__ == "__main__":
    main()
