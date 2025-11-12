"""
E-Commerce Data Cleaning Pipeline
==================================
This script cleans raw e-commerce data from Olist dataset.

Cleaning steps:
1. Load raw CSV files
2. Handle missing values
3. Convert data types (especially dates!)
4. Remove duplicates
5. Standardize text formatting
6. Validate data ranges
7. Export cleaned data

"""

import pandas as pd
import numpy as np
from datetime import datetime
import os

pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

RAW_DATA_PATH = '../data/raw'
CLEAN_DATA_PATH = '../data/cleaned'

# Create cleaned folder if it doesn't exist
os.makedirs(CLEAN_DATA_PATH, exist_ok=True)

print("="*70)
print("🧹 E-COMMERCE DATA CLEANING PIPELINE")
print("="*70)
print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

def load_raw_data():
    """Load all raw CSV files into pandas DataFrame"""

    print("📂 STEP 1: Loading raw data...")
    print("-" * 70)

    #Load main datasets
    customers = pd.read_csv(RAW_DATA_PATH + 'olist_customers_dataset.csv')
    orders = pd.read_csv(RAW_DATA_PATH + 'olist_orders_dataset.csv')
    order_items = pd.read_csv(RAW_DATA_PATH + 'olist_order_items_dataset.csv')
    products = pd.read_csv(RAW_DATA_PATH + 'olist_products_dataset.csv')
    
    print(f"✅ Customers: {len(customers):,} rows, {len(customers.columns)} columns")
    print(f"✅ Orders: {len(orders):,} rows, {len(orders.columns)} columns")
    print(f"✅ Order Items: {len(order_items):,} rows, {len(order_items.columns)} columns")
    print(f"✅ Products: {len(products):,} rows, {len(products.columns)} columns")
    print()
    
    return customers, orders, order_items, products

def handle_missing_values(df, dataset_name):
    """
    Handling missing values in DataFrame
    
    Parameters:
    -df: pandas DataFrame
    -dataset_name: str, name for logging
    
    Returns:
    -Cleaned DataFrame
    """
 
    print(f"🔍 STEP 2: Handling missing values in {dataset_name}...")
    print("-" * 70)    

    #Count missing values
    missing_before = df.isnull().sum().sum()
    print(f"Missing values before: {missing_before: ,}")

    for col in df.columns:
        missing_count = df[col].isnull().sum()
        missing_pct = (missing_count / len(df)) * 100

        if missing_count > 0:
            print(f" - {col}: {missing_count: ,} missing ({missing_pct:.1f}%)")

            # If < 5% missing, drop rows
            if missing_pct < 5:
                df = df.dropna(subset=[col])
                print(f"   -> Dropped cols (< 5% missing)")

            # If categorical, fill with Unknown
            elif df[col].dtype == 'object':
                df[col].fillna('Unknown', inplace=True)
                print(f"   -> Filled with 'Unknown' ")

            # If numerical, fill with median
            else:
                median_val = df[col].median()
                df[col].fillna(median_val, inplace=True)
                print(f".  -> Filled with median({median_val:.2f})")

    # Count missing after
    missing_after = df.isnull().sum().sum()
    print(f"\nMissing values after: {missing_after:,}")
    print(f"✅ Cleaned {missing_before - missing_after:,} missing values\n")