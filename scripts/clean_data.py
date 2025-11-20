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

RAW_DATA_PATH = '../data/raw/'
CLEAN_DATA_PATH = '../data/cleaned/'

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
    
    return df

def convert_date_columns(df, dataset_name, date_columns):
    """
    Convert date columns from text to datetime format
    
    Parameters:
    - df: pandas DataFrame
    - dataset_name: str, name for logging
    - date_columns: list of column names to convert
        
    Returns:
    - DataFrame with converted dates
    """
    
    print(f"📅 STEP 3: Converting date columns in {dataset_name}...")
    print("-" * 70)    

    for col in date_columns:
        if col in df.columns:
            print(f" Converting {col}...")    
            print(f" Before: {df[col].dtype}")
            df[col] = pd.to_datetime(df[col], errors='coerce')
            print(f"After: {df[col].dtype}")

            # Count how many failed to convert and became NaT
            nat_count = df[col].isnull().sum()
            if nat_count > 0:
                print(f"    ⚠️  {nat_count} values couldn't be converted (now NaT)")
            else:
                print(f"    ✅ All values converted successfully")
    print()
    return df

def remove_duplicates(df, dataset_name, subset=None):
    """
    Remove duplicate rows from DataFrame
    
    Parameters:
    - df: pandas DataFrame
    - dataset_name: str, name for logging
    - subset: list of columns to check for duplicates (None = all columns)
    
    Returns:
    - DataFrame with duplicates removed
    """

    print(f"🔍 STEP 4: Removing duplicates from {dataset_name}...")
    print("-" * 70)
    
    # Count before
    rows_before = len(df)
    
    # Find duplicates
    if subset:
        duplicates = df.duplicated(subset=subset, keep='first')
        print(f"  Checking duplicates based on: {subset}")
    else:
        duplicates = df.duplicated(keep='first')
        print(f"  Checking duplicates based on: all columns")
    
    duplicate_count = duplicates.sum()
    print(f"  Found {duplicate_count:,} duplicate rows")
    
    # Remove duplicates
    if duplicate_count > 0:
        df = df.drop_duplicates(subset=subset, keep='first')
        rows_after = len(df)
        print(f"  ✅ Removed {rows_before - rows_after:,} duplicate rows")
    else:
        print(f"  ✅ No duplicates found")
    
    print()
    return df

def standardize_text(df, dataset_name, text_columns):
    """
    Standardize text columns (lowercase, strip whitespace)
    
    Parameters:
    - df: pandas DataFrame
    - dataset_name: str, name for logging
    - text_columns: list of column names to standardize
    
    Returns:
    - DataFrame with standardized text
    """
    
    print(f"✏️  STEP 5: Standardizing text in {dataset_name}...")
    print("-" * 70)   

    for col in text_columns:
        if col in df.columns and df[col].dtype == 'object':
            print(f"  Standardizing {col}...")
            
            # Show example before
            sample_before = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
            
            # Convert to string, lowercase, strip whitespace
            df[col] = df[col].astype(str).str.lower().str.strip()
            
            # Show example after
            sample_after = df[col].iloc[0] if not df[col].empty else None
            
            print(f"    Before: '{sample_before}'")
            print(f"    After:  '{sample_after}'")
            print(f"    ✅ Standardized")
    
    print()
    return df   

def validate_data(df, dataset_name):
    """
    Validate data ranges and business rules
    
    Parameters:
    - df: pandas DataFrame
    - dataset_name: str, name for logging
    
    Returns:
    - DataFrame with invalid data handled
    """
    
    print(f"✔️  STEP 6: Validating data in {dataset_name}...")
    print("-" * 70)
    
    # Check for negative values in numeric columns that should be positive
    numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
    
    for col in numeric_cols:
        if 'price' in col.lower() or 'amount' in col.lower() or 'quantity' in col.lower():
            negative_count = (df[col] < 0).sum()
            
            if negative_count > 0:
                print(f"  ⚠️  {col}: Found {negative_count} negative values")
                # Remove rows with negative values
                df = df[df[col] >= 0]
                print(f"    → Removed {negative_count} rows with negative values")
    
    # Check for outliers (values > 3 standard deviations from mean)
    for col in numeric_cols:
        mean = df[col].mean()
        std = df[col].std()
        outlier_threshold = mean + (3 * std)
        outliers = df[col] > outlier_threshold
        outlier_count = outliers.sum()
        
        if outlier_count > 0:
            print(f"  ⚠️  {col}: Found {outlier_count} outliers (> {outlier_threshold:.2f})")
            # Flag but don't remove (outliers might be valid)
    
    print(f"  ✅ Validation complete\n")
    return df

def main():
    """Main function to run entire cleaning pipeline"""
    
    # Step 1: Load data
    customers, orders, order_items, products = load_raw_data()
    
    # Step 2: Clean orders (most important dataset)
    print("\n" + "="*70)
    print("🧹 CLEANING ORDERS DATASET")
    print("="*70 + "\n")
    
    # Handle missing values
    orders = handle_missing_values(orders, "orders")
    
    # Convert date columns
    date_cols = ['order_purchase_timestamp', 'order_approved_at', 
                 'order_delivered_carrier_date', 'order_delivered_customer_date',
                 'order_estimated_delivery_date']
    orders = convert_date_columns(orders, "orders", date_cols)
    
    # Remove duplicates (based on order_id)
    orders = remove_duplicates(orders, "orders", subset=['order_id'])
    
    # Standardize text
    text_cols = ['order_status']
    orders = standardize_text(orders, "orders", text_cols)
    
    # Validate data
    orders = validate_data(orders, "orders")
    
    # Step 3: Clean customers
    print("\n" + "="*70)
    print("🧹 CLEANING CUSTOMERS DATASET")
    print("="*70 + "\n")
    
    customers = handle_missing_values(customers, "customers")
    customers = remove_duplicates(customers, "customers", subset=['customer_id'])
    
    text_cols = ['customer_city', 'customer_state']
    customers = standardize_text(customers, "customers", text_cols)
    
    # Step 4: Clean products
    print("\n" + "="*70)
    print("🧹 CLEANING PRODUCTS DATASET")
    print("="*70 + "\n")
    
    products = handle_missing_values(products, "products")
    products = remove_duplicates(products, "products", subset=['product_id'])
    
    # Step 5: Clean order_items
    print("\n" + "="*70)
    print("🧹 CLEANING ORDER ITEMS DATASET")
    print("="*70 + "\n")
    
    order_items = handle_missing_values(order_items, "order_items")
    order_items = remove_duplicates(order_items, "order_items")
    order_items = validate_data(order_items, "order_items")
    
    # Step 6: Save cleaned data
    print("\n" + "="*70)
    print("💾 SAVING CLEANED DATA")
    print("="*70 + "\n")
    
    customers.to_csv(CLEAN_DATA_PATH + 'customers_clean.csv', index=False)
    print(f"✅ Saved customers_clean.csv ({len(customers):,} rows)")
    
    orders.to_csv(CLEAN_DATA_PATH + 'orders_clean.csv', index=False)
    print(f"✅ Saved orders_clean.csv ({len(orders):,} rows)")
    
    order_items.to_csv(CLEAN_DATA_PATH + 'order_items_clean.csv', index=False)
    print(f"✅ Saved order_items_clean.csv ({len(order_items):,} rows)")
    
    products.to_csv(CLEAN_DATA_PATH + 'products_clean.csv', index=False)
    print(f"✅ Saved products_clean.csv ({len(products):,} rows)")
    
    # Summary
    print("\n" + "="*70)
    print("✨ CLEANING COMPLETE!")
    print("="*70)
    print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"\nCleaned data saved to: {CLEAN_DATA_PATH}")
    print("\n🎉 Data is ready for database loading!")


if __name__ == "__main__":
    main()