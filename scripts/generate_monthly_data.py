"""
Monthly Bakehouse Data Generator
Generates realistic sales data for a specified month with proper relational integrity.

Usage:
    python scripts/generate_monthly_data.py --month 2025-12
    python scripts/generate_monthly_data.py --month 2026-01 --customers 500 --transactions 5000
"""

import argparse
import os
import random
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import List, Dict
from uuid import uuid4

from dotenv import load_dotenv
from faker import Faker
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session, declarative_base
from sqlalchemy import Column, String, Integer, Numeric, DateTime, ForeignKey, Text, Date
from sqlalchemy.dialects.postgresql import UUID

# Load environment variables from .env file
load_dotenv()

# Initialize Faker for realistic mock data
fake = Faker()
Faker.seed(42)  # Reproducible data for testing
random.seed(42)

# SQLAlchemy Base
Base = declarative_base()

# ============================================================================
# ORM Models - Mirror the PostgreSQL schema
# ============================================================================

class Customer(Base):
    """Customer model - parent entity"""
    __tablename__ = 'customers'
    __table_args__ = {'schema': 'prod'}
    
    customer_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    email = Column(Text, unique=True)
    first_name = Column(Text)
    last_name = Column(Text)
    phone = Column(Text)
    loyalty_status = Column(Text, nullable=False)
    loyalty_joined_at = Column(DateTime(timezone=True))
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow)


class Transaction(Base):
    """Transaction model - parent to transaction_items, child to customers"""
    __tablename__ = 'transactions'
    __table_args__ = {'schema': 'prod'}
    
    transaction_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    customer_id = Column(UUID(as_uuid=True), ForeignKey('prod.customers.customer_id'), nullable=True)
    transaction_datetime = Column(DateTime(timezone=True), nullable=False)
    transaction_date = Column(Date, nullable=False)  # Derived from datetime via trigger
    store_id = Column(Text)
    register_id = Column(Text)
    payment_method = Column(Text)
    subtotal = Column(Numeric(12, 2), nullable=False)
    tax = Column(Numeric(12, 2), nullable=False, default=0)
    total = Column(Numeric(12, 2), nullable=False)
    currency_code = Column(String(3), nullable=False, default='USD')
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow)


class TransactionItem(Base):
    """Transaction line items - child entity"""
    __tablename__ = 'transaction_items'
    __table_args__ = {'schema': 'prod'}
    
    transaction_id = Column(UUID(as_uuid=True), ForeignKey('prod.transactions.transaction_id'), primary_key=True)
    line_number = Column(Integer, primary_key=True)
    product_id = Column(Text, nullable=False)
    product_name = Column(Text)
    quantity = Column(Numeric(12, 3), nullable=False)
    unit_price = Column(Numeric(12, 2), nullable=False)
    discount_amount = Column(Numeric(12, 2), nullable=False, default=0)
    unit_cost = Column(Numeric(12, 4))
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow)


# ============================================================================
# Product Catalog - Fixed set of bakery products
# ============================================================================

BAKERY_PRODUCTS = [
    # Breads
    {"id": "PROD001", "name": "Sourdough Loaf", "price": 6.50, "cost": 2.80},
    {"id": "PROD002", "name": "Whole Wheat Bread", "price": 5.50, "cost": 2.30},
    {"id": "PROD003", "name": "Baguette", "price": 4.00, "cost": 1.60},
    {"id": "PROD004", "name": "Ciabatta", "price": 5.00, "cost": 2.00},
    {"id": "PROD005", "name": "Rye Bread", "price": 6.00, "cost": 2.50},
    {"id": "PROD006", "name": "Multigrain Loaf", "price": 6.50, "cost": 2.70},
    {"id": "PROD007", "name": "Focaccia", "price": 7.00, "cost": 3.00},
    {"id": "PROD008", "name": "Brioche", "price": 8.00, "cost": 3.50},
    
    # Pastries
    {"id": "PROD009", "name": "Croissant", "price": 3.50, "cost": 1.20},
    {"id": "PROD010", "name": "Pain au Chocolat", "price": 4.00, "cost": 1.50},
    {"id": "PROD011", "name": "Almond Croissant", "price": 4.50, "cost": 1.80},
    {"id": "PROD012", "name": "Danish Pastry", "price": 4.00, "cost": 1.60},
    {"id": "PROD013", "name": "Cinnamon Roll", "price": 4.50, "cost": 1.70},
    {"id": "PROD014", "name": "Apple Turnover", "price": 4.00, "cost": 1.50},
    {"id": "PROD015", "name": "Éclair", "price": 5.00, "cost": 2.00},
    {"id": "PROD016", "name": "Macaron (box of 6)", "price": 12.00, "cost": 5.00},
    
    # Cakes & Desserts
    {"id": "PROD017", "name": "Chocolate Cake Slice", "price": 6.00, "cost": 2.40},
    {"id": "PROD018", "name": "Carrot Cake Slice", "price": 6.50, "cost": 2.60},
    {"id": "PROD019", "name": "Cheesecake Slice", "price": 7.00, "cost": 3.00},
    {"id": "PROD020", "name": "Tiramisu", "price": 7.50, "cost": 3.20},
    {"id": "PROD021", "name": "Fruit Tart", "price": 6.00, "cost": 2.50},
    {"id": "PROD022", "name": "Lemon Meringue Pie Slice", "price": 5.50, "cost": 2.20},
    {"id": "PROD023", "name": "Brownie", "price": 4.00, "cost": 1.50},
    {"id": "PROD024", "name": "Cookie (each)", "price": 2.50, "cost": 0.80},
    
    # Beverages
    {"id": "PROD025", "name": "Espresso", "price": 3.00, "cost": 0.60},
    {"id": "PROD026", "name": "Cappuccino", "price": 4.50, "cost": 1.00},
    {"id": "PROD027", "name": "Latte", "price": 5.00, "cost": 1.20},
    {"id": "PROD028", "name": "Americano", "price": 3.50, "cost": 0.70},
    {"id": "PROD029", "name": "Mocha", "price": 5.50, "cost": 1.40},
    {"id": "PROD030", "name": "Hot Chocolate", "price": 4.50, "cost": 1.10},
    {"id": "PROD031", "name": "Tea", "price": 3.00, "cost": 0.50},
    {"id": "PROD032", "name": "Iced Coffee", "price": 4.50, "cost": 1.00},
    {"id": "PROD033", "name": "Fresh Orange Juice", "price": 5.00, "cost": 1.50},
    
    # Sandwiches & Savory
    {"id": "PROD034", "name": "Ham & Cheese Croissant", "price": 7.00, "cost": 3.00},
    {"id": "PROD035", "name": "Turkey Sandwich", "price": 8.50, "cost": 3.50},
    {"id": "PROD036", "name": "Caprese Sandwich", "price": 8.00, "cost": 3.20},
    {"id": "PROD037", "name": "Quiche Lorraine", "price": 6.50, "cost": 2.80},
    {"id": "PROD038", "name": "Spinach & Feta Pastry", "price": 5.50, "cost": 2.20},
    {"id": "PROD039", "name": "Sausage Roll", "price": 5.00, "cost": 2.00},
    
    # Specialty Items
    {"id": "PROD040", "name": "Bagel with Cream Cheese", "price": 5.00, "cost": 1.80},
    {"id": "PROD041", "name": "Muffin (Blueberry)", "price": 3.50, "cost": 1.20},
    {"id": "PROD042", "name": "Muffin (Chocolate Chip)", "price": 3.50, "cost": 1.20},
    {"id": "PROD043", "name": "Scone", "price": 3.50, "cost": 1.10},
    {"id": "PROD044", "name": "Pretzel", "price": 4.00, "cost": 1.30},
    {"id": "PROD045", "name": "Donut (Glazed)", "price": 2.50, "cost": 0.80},
    {"id": "PROD046", "name": "Donut (Filled)", "price": 3.00, "cost": 1.00},
    {"id": "PROD047", "name": "Cupcake", "price": 4.00, "cost": 1.50},
    {"id": "PROD048", "name": "Biscotti (3 pack)", "price": 5.00, "cost": 1.80},
    {"id": "PROD049", "name": "Granola Bar", "price": 3.00, "cost": 1.00},
    {"id": "PROD050", "name": "Fruit Cup", "price": 4.50, "cost": 1.80},
]

# Store and Register Configuration
STORES = [
    {"store_id": "STORE001", "name": "Downtown Bakehouse", "registers": ["REG001", "REG002"]},
    {"store_id": "STORE002", "name": "Westside Bakehouse", "registers": ["REG003", "REG004"]},
    {"store_id": "STORE003", "name": "Eastside Bakehouse", "registers": ["REG005", "REG006"]},
]

PAYMENT_METHODS = ["cash", "credit_card", "debit_card", "mobile_payment"]
LOYALTY_STATUSES = ["none", "member", "vip"]

# ============================================================================
# Data Generation Functions
# ============================================================================

def generate_customers(count: int) -> List[Customer]:
    """
    Generate realistic customer records.
    
    Educational Note: Customers are generated FIRST because they are parent entities.
    Transaction records will reference these customer_ids via foreign keys.
    """
    print(f"Generating {count} customers...")
    customers = []
    
    for i in range(count):
        loyalty_status = random.choices(
            LOYALTY_STATUSES,
            weights=[0.3, 0.5, 0.2],  # 30% no loyalty, 50% member, 20% VIP
            k=1
        )[0]
        
        # VIP and members have loyalty_joined_at dates
        loyalty_joined = None
        if loyalty_status in ["member", "vip"]:
            days_ago = random.randint(30, 730)  # Joined 1 month to 2 years ago
            loyalty_joined = datetime.now(timezone.utc) - timedelta(days=days_ago)

        # Ensure email uniqueness across repeated script runs (customers.email is UNIQUE)
        base_email = fake.email().lower()
        local_part, domain_part = base_email.split('@', 1)
        unique_email = f"{local_part}+{uuid4().hex[:8]}@{domain_part}"
        
        customer = Customer(
            customer_id=uuid4(),
            email=unique_email,
            first_name=fake.first_name(),
            last_name=fake.last_name(),
            phone=fake.phone_number()[:20],  # Limit length
            loyalty_status=loyalty_status,
            loyalty_joined_at=loyalty_joined,
        )
        customers.append(customer)
    
    print(f"✓ Generated {len(customers)} customers")
    return customers


def generate_transactions_and_items(
    customer_ids: List[str],
    target_month: str,
    transaction_count: int
) -> tuple[List[Transaction], List[TransactionItem]]:
    """
    Generate transactions and their line items for a specific month.
    
    Educational Note: This demonstrates DATA INTERDEPENDENCY:
    1. We generate Transaction records FIRST (parents)
    2. We store their transaction_ids in memory
    3. We then generate TransactionItem records (children) that reference those IDs
    
    Why? Because of FOREIGN KEY CONSTRAINTS:
    - transaction_items.transaction_id MUST exist in transactions.transaction_id
    - If we try to insert a child record before its parent, PostgreSQL will reject it
    
    SQLAlchemy simplifies this by letting us define the relationship once in the model,
    then manage the FK references through Python objects rather than manual SQL JOINs.
    """
    print(f"Generating {transaction_count} transactions for {target_month}...")
    
    # Parse target month
    year, month = map(int, target_month.split('-'))
    
    # Calculate date range for the month
    start_date = datetime(year, month, 1)
    if month == 12:
        end_date = datetime(year + 1, 1, 1) - timedelta(seconds=1)
    else:
        end_date = datetime(year, month + 1, 1) - timedelta(seconds=1)
    
    transactions = []
    all_items = []
    
    for i in range(transaction_count):
        # Random datetime within the month, weighted toward business hours (7am-8pm)
        days_offset = random.randint(0, (end_date - start_date).days)
        hour = random.choices(
            range(6, 21),  # 6am to 8pm
            weights=[1, 3, 5, 4, 3, 4, 5, 6, 7, 6, 5, 4, 3, 2, 1],  # Peak at lunch/afternoon
            k=1
        )[0]
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        
        transaction_datetime = start_date + timedelta(
            days=days_offset,
            hours=hour,
            minutes=minute,
            seconds=second
        )
        
        # Assign customer (70% have customer_id, 30% are guest checkouts)
        customer_id = random.choice(customer_ids) if random.random() < 0.7 else None
        
        # Random store and register
        store = random.choice(STORES)
        register = random.choice(store["registers"])
        
        # Generate transaction ID
        transaction_id = uuid4()
        
        # Generate 1-8 line items for this transaction
        num_items = random.choices(
            range(1, 9),
            weights=[5, 15, 20, 25, 20, 10, 3, 2],  # Most common: 3-5 items
            k=1
        )[0]
        
        items = []
        subtotal = Decimal('0.00')
        
        for line_num in range(1, num_items + 1):
            product = random.choice(BAKERY_PRODUCTS)
            
            # Quantity: mostly 1-2, occasionally more
            quantity = random.choices([1, 2, 3, 4], weights=[60, 25, 10, 5], k=1)[0]
            
            # Price and cost
            unit_price = Decimal(str(product["price"]))
            unit_cost = Decimal(str(product["cost"]))
            
            # 10% chance of discount
            discount_amount = Decimal('0.00')
            if random.random() < 0.1:
                discount_amount = round(unit_price * Decimal(str(random.uniform(0.05, 0.20))), 2)
            
            line_total = (Decimal(quantity) * unit_price) - discount_amount
            subtotal += line_total
            
            item = TransactionItem(
                transaction_id=transaction_id,
                line_number=line_num,
                product_id=product["id"],
                product_name=product["name"],
                quantity=Decimal(quantity),
                unit_price=unit_price,
                discount_amount=discount_amount,
                unit_cost=unit_cost,
            )
            items.append(item)
        
        # Calculate tax (8% for example)
        tax = round(subtotal * Decimal('0.08'), 2)
        total = subtotal + tax
        
        transaction = Transaction(
            transaction_id=transaction_id,
            customer_id=customer_id,
            transaction_datetime=transaction_datetime,
            transaction_date=transaction_datetime.date(),
            store_id=store["store_id"],
            register_id=register,
            payment_method=random.choice(PAYMENT_METHODS),
            subtotal=subtotal,
            tax=tax,
            total=total,
        )
        
        transactions.append(transaction)
        all_items.extend(items)
        
        if (i + 1) % 1000 == 0:
            print(f"  Generated {i + 1}/{transaction_count} transactions...")
    
    print(f"✓ Generated {len(transactions)} transactions with {len(all_items)} line items")
    return transactions, all_items


def batch_insert(session: Session, objects: List, batch_size: int = 500):
    """
    Insert objects in batches for better performance.
    
    Educational Note: Inserting 50,000+ rows one-by-one is slow due to network
    round-trips and transaction overhead. Batching reduces this significantly.
    SQLAlchemy's bulk_save_objects() is optimized for mass inserts.
    """
    total = len(objects)
    for i in range(0, total, batch_size):
        batch = objects[i:i + batch_size]
        session.bulk_save_objects(batch)
        session.commit()
        print(f"  Inserted {min(i + batch_size, total)}/{total} records")


# ============================================================================
# Main Execution
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="Generate monthly bakehouse sales data")
    parser.add_argument(
        '--month',
        required=True,
        help='Target month in YYYY-MM format (e.g., 2025-12)'
    )
    parser.add_argument(
        '--customers',
        type=int,
        default=1000,
        help='Number of customers to generate (default: 1000)'
    )
    parser.add_argument(
        '--transactions',
        type=int,
        default=12500,
        help='Number of transactions to generate (default: 12500)'
    )
    
    args = parser.parse_args()
    
    # Validate month format
    try:
        datetime.strptime(args.month, '%Y-%m')
    except ValueError:
        print("Error: Month must be in YYYY-MM format (e.g., 2025-12)")
        sys.exit(1)
    
    # Get database URL from environment
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        print("Error: DATABASE_URL environment variable not set")
        print("Set it in your .env file or environment:")
        print("  DATABASE_URL='postgresql://user:pass@host:port/bakehouse_dev'")
        sys.exit(1)

    # If URL uses plain postgresql://, SQLAlchemy defaults to psycopg2.
    # We use psycopg3, so normalize to postgresql+psycopg:// automatically.
    if database_url.startswith('postgresql://') and not database_url.startswith('postgresql+psycopg://'):
        database_url = database_url.replace('postgresql://', 'postgresql+psycopg://', 1)
    
    print(f"\n{'='*70}")
    print(f"Bakehouse Monthly Data Generator")
    print(f"{'='*70}")
    print(f"Target Month: {args.month}")
    print(f"Customers: {args.customers:,}")
    print(f"Transactions: {args.transactions:,}")
    print(f"Expected Line Items: ~{args.transactions * 4:,} (avg 4 per transaction)")
    print(f"{'='*70}\n")
    
    # Create SQLAlchemy engine and session
    engine = create_engine(database_url, echo=False)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Step 1: Generate customers (parent entities)
        customers = generate_customers(args.customers)
        print("\nInserting customers into database...")
        batch_insert(session, customers)
        
        # Extract customer IDs for transaction generation
        customer_ids = [str(c.customer_id) for c in customers]
        
        # Step 2: Generate transactions and items (maintaining FK relationships)
        transactions, items = generate_transactions_and_items(
            customer_ids,
            args.month,
            args.transactions
        )
        
        print("\nInserting transactions into database...")
        batch_insert(session, transactions)
        
        print("\nInserting transaction items into database...")
        batch_insert(session, items)
        
        # Verify counts
        print("\n" + "="*70)
        print("Verification:")
        print("="*70)
        
        customer_count = session.execute(text("SELECT COUNT(*) FROM prod.customers")).scalar()
        transaction_count = session.execute(text("SELECT COUNT(*) FROM prod.transactions")).scalar()
        item_count = session.execute(text("SELECT COUNT(*) FROM prod.transaction_items")).scalar()
        
        print(f"✓ Customers in database: {customer_count:,}")
        print(f"✓ Transactions in database: {transaction_count:,}")
        print(f"✓ Transaction items in database: {item_count:,}")
        print(f"✓ Average items per transaction: {item_count / transaction_count:.2f}")
        
        print("\n" + "="*70)
        print("✓ Data generation complete!")
        print("="*70 + "\n")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        session.rollback()
        sys.exit(1)
    finally:
        session.close()


if __name__ == '__main__':
    main()
