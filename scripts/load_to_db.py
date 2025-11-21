"""
Load Cleaned Data to PostgreSQL
================================
This script loads cleaned CSV data into PostgreSQL star schema.

Tables:
- dim_customers (customer dimension)
- dim_products (product dimension)  
- dim_date (date dimension)
- fact_orders (transaction fact)

"""

import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import os

# PostgreSQL connection details
DB_USER = os.getenv('USER')  # Your Mac username
DB_PASSWORD = ''              # Empty for local PostgreSQL
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'ecommerce_dw'

# Data path
CLEAN_DATA_PATH = '../data/cleaned/'

print("="*70)
print("📥 LOADING DATA TO POSTGRESQL")
print("="*70)
print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

def create_connection():
    """Create SQLAlchemy engine for PostgreSQL"""
    
    # Build connection string
    conn_string = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    
    print(f"🔌 Connecting to PostgreSQL...")
    print(f"   Database: {DB_NAME}")
    print(f"   Host: {DB_HOST}:{DB_PORT}")
    
    # Create engine
    engine = create_engine(conn_string)
    
    # Test connection
    try:
        with engine.connect() as conn:
            print(f"   ✅ Connected successfully!\n")
    except Exception as e:
        print(f"   ❌ Connection failed: {e}")
        raise
    
    return engine

def load_customers(engine):
    """Load customers dimension table"""
    
    print("👥 Loading customers dimension...")
    print("-" * 70)
    
    # Read cleaned CSV
    df = pd.read_csv(CLEAN_DATA_PATH + 'customers_clean.csv')
    print(f"  📄 Loaded {len(df):,} customers from CSV")
    
    # Select columns matching schema
    df_clean = df[['customer_id', 'customer_city', 'customer_state', 
                   'customer_zip_code_prefix']].copy()
    
    # Load to PostgreSQL
    rows_loaded = df_clean.to_sql('dim_customers', engine, 
                                   if_exists='append', index=False)
    
    print(f"  ✅ Inserted {len(df_clean):,} rows into dim_customers\n")
    return len(df_clean)

def load_products(engine):
    """Load products dimension table"""
    
    print("📦 Loading products dimension...")
    print("-" * 70)
    
    df = pd.read_csv(CLEAN_DATA_PATH + 'products_clean.csv')
    print(f"  📄 Loaded {len(df):,} products from CSV")
    
    # Select columns (handle misspelling in CSV: lenght vs length)
    df_clean = df[['product_id', 'product_category_name', 'product_name_lenght',
                   'product_description_lenght', 'product_photos_qty',
                   'product_weight_g', 'product_length_cm', 'product_height_cm',
                   'product_width_cm']].copy()
    
    df_clean.to_sql('dim_products', engine, if_exists='append', index=False)
    
    print(f"  ✅ Inserted {len(df_clean):,} rows into dim_products\n")
    return len(df_clean)


def generate_date_dimension(engine, start_date='2016-01-01', end_date='2018-12-31'):
    """Generate and load date dimension"""
    
    print("📅 Generating date dimension...")
    print("-" * 70)
    
    # Create date range
    dates = pd.date_range(start=start_date, end=end_date, freq='D')
    
    print(f"  📅 Generating dates from {start_date} to {end_date}")
    
    # Build dimension
    date_dim = pd.DataFrame({
        'date': dates,
        'year': dates.year,
        'quarter': dates.quarter,
        'month': dates.month,
        'month_name': dates.month_name(),
        'day': dates.day,
        'day_of_week': dates.dayofweek + 1,
        'day_name': dates.day_name(),
        'week_of_year': dates.isocalendar().week,
        'is_weekend': dates.dayofweek >= 5
    })
    
    # Load to database
    date_dim.to_sql('dim_date', engine, if_exists='append', index=False)
    
    print(f"  ✅ Generated and inserted {len(date_dim):,} date records\n")
    return len(date_dim)

def load_fact_orders(engine):
    """Load fact orders table with dimensional keys"""
    
    print("📊 Loading fact orders (transactions)...")
    print("-" * 70)
    
    # Load cleaned data
    orders = pd.read_csv(CLEAN_DATA_PATH + 'orders_clean.csv')
    order_items = pd.read_csv(CLEAN_DATA_PATH + 'order_items_clean.csv')
    
    print(f"  📄 Loaded {len(orders):,} orders")
    print(f"  📄 Loaded {len(order_items):,} order items")
    
    # Parse dates (they're strings in CSV)
    date_cols = ['order_purchase_timestamp', 'order_approved_at', 
                 'order_delivered_carrier_date', 'order_delivered_customer_date',
                 'order_estimated_delivery_date']
    
    for col in date_cols:
        if col in orders.columns:
            orders[col] = pd.to_datetime(orders[col])
    
    print("  ✅ Converted date columns")
    
    # Merge orders with order_items
    fact_data = order_items.merge(
        orders[['order_id', 'customer_id', 'order_status', 'order_purchase_timestamp',
                'order_approved_at', 'order_delivered_carrier_date', 
                'order_delivered_customer_date', 'order_estimated_delivery_date']],
        on='order_id',
        how='left'
    )
    
    print(f"  ✅ Merged orders with order_items: {len(fact_data):,} rows")
    
    # Get dimension keys from database
    print("  🔗 Looking up dimension keys...")
    
    # Get customer keys
    customers_dim = pd.read_sql("SELECT customer_key, customer_id FROM dim_customers", engine)
    fact_data = fact_data.merge(customers_dim, on='customer_id', how='left')
    
    # Get product keys
    products_dim = pd.read_sql("SELECT product_key, product_id FROM dim_products", engine)
    fact_data = fact_data.merge(products_dim, on='product_id', how='left')
    
    # Get date keys (based on order_purchase_timestamp)
    fact_data['order_date'] = fact_data['order_purchase_timestamp'].dt.date
    dates_dim = pd.read_sql("SELECT date_key, date FROM dim_date", engine)
    fact_data = fact_data.merge(dates_dim, left_on='order_date', right_on='date', how='left')
    # Rename date_key to order_date_key to match schema
    fact_data = fact_data.rename(columns={'date_key': 'order_date_key'})
    print("  ✅ Mapped to dimension keys")
    
    # Select final columns for fact table
    fact_final = fact_data[[
        'order_id', 'customer_key', 'product_key', 'order_date_key',
        'order_item_id', 'price', 'freight_value', 
        'order_status', 'seller_id',
        'order_purchase_timestamp', 'order_approved_at',
        'order_delivered_carrier_date', 'order_delivered_customer_date',
        'order_estimated_delivery_date'
    ]].copy()
    
    # Check for nulls in keys (shouldn't happen!)
    null_customers = fact_final['customer_key'].isnull().sum()
    null_products = fact_final['product_key'].isnull().sum()
    null_dates = fact_final['order_date_key'].isnull().sum()
    
    if null_customers > 0:
        print(f"  ⚠️  Warning: {null_customers} rows missing customer_key")
    if null_products > 0:
        print(f"  ⚠️  Warning: {null_products} rows missing product_key")
    if null_dates > 0:
        print(f"  ⚠️  Warning: {null_dates} rows missing date_key")
    
    # Remove rows with missing keys
    fact_final = fact_final.dropna(subset=['customer_key', 'product_key', 'order_date_key'])
    
    print(f"  📊 Final fact table: {len(fact_final):,} rows")
    
    # Load to database
    fact_final.to_sql('fact_orders', engine, if_exists='append', index=False)
    
    print(f"  ✅ Inserted {len(fact_final):,} rows into fact_orders\n")
    return len(fact_final)

def main():
    """Load all data to PostgreSQL"""
    
    # Create connection
    engine = create_connection()
    
    try:
        # Load dimensions FIRST
        print("="*70)
        print("📊 LOADING DIMENSION TABLES")
        print("="*70 + "\n")
        
        customers_count = load_customers(engine)
        products_count = load_products(engine)
        dates_count = generate_date_dimension(engine)
        
        # Load fact table AFTER dimensions
        print("="*70)
        print("📊 LOADING FACT TABLE")
        print("="*70 + "\n")
        
        orders_count = load_fact_orders(engine)  # ← ADD THIS LINE
        
        # Summary
        print("="*70)
        print("✨ LOADING COMPLETE!")
        print("="*70)
        print(f"\n📊 Summary:")
        print(f"   Customers: {customers_count:,} rows")
        print(f"   Products: {products_count:,} rows")
        print(f"   Dates: {dates_count:,} rows")
        print(f"   Orders: {orders_count:,} rows")  # ← ADD THIS LINE
        print(f"\n🎉 Star schema complete!")
        print(f"   Database: {DB_NAME}")
        print(f"   Connect with: psql {DB_NAME}")
        
    except Exception as e:
        print(f"\n❌ Error during loading: {e}")
        import traceback
        traceback.print_exc()
    finally:
        engine.dispose()
        print(f"\n   Connection closed.")

if __name__ == "__main__":
    main()