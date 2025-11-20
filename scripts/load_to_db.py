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

def main():
    """Load all data to PostgreSQL"""
    
    # Create connection
    engine = create_connection()
    
    try:
        # Load dimensions FIRST (fact table references them!)
        print("="*70)
        print("📊 LOADING DIMENSION TABLES")
        print("="*70 + "\n")
        
        customers_count = load_customers(engine)
        products_count = load_products(engine)
        dates_count = generate_date_dimension(engine)
        
        # Summary
        print("="*70)
        print("✨ LOADING COMPLETE!")
        print("="*70)
        print(f"\n📊 Summary:")
        print(f"   Customers: {customers_count:,} rows")
        print(f"   Products: {products_count:,} rows")
        print(f"   Dates: {dates_count:,} rows")
        print(f"\n🎉 Data warehouse ready for analysis!")
        print(f"   Database: {DB_NAME}")
        print(f"   Connect with: psql {DB_NAME}")
        
    except Exception as e:
        print(f"\n❌ Error during loading: {e}")
        raise
    finally:
        engine.dispose()
        print(f"\n   Connection closed.")


if __name__ == "__main__":
    main()