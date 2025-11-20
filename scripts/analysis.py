"""
E-Commerce Business Analysis
=============================
SQL queries to analyze e-commerce data and generate business insights.

Metrics calculated:
- Customer statistics
- Product performance
- Revenue analysis
- Time-based trends

"""

import pandas as pd
from sqlalchemy import create_engine
import os

# Database connection
DB_USER = os.getenv('USER')
DB_PASSWORD = ''
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'ecommerce_dw'

# Create connection
conn_string = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
engine = create_engine(conn_string)

print("="*70)
print("📊 E-COMMERCE BUSINESS ANALYSIS")
print("="*70)
print()

# Query 1: Customer Distribution by State
print("🗺️  CUSTOMER DISTRIBUTION BY STATE")
print("-"*70)

query1 = """
SELECT 
    customer_state,
    COUNT(*) as customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM dim_customers
GROUP BY customer_state
ORDER BY customer_count DESC
LIMIT 10;
"""

result1 = pd.read_sql(query1, engine)
print(result1.to_string(index=False))
print()

# Query 2: Top 10 Product Categories by Sales Volume
print("📦 TOP 10 PRODUCT CATEGORIES")
print("-"*70)

# Query 2: Product Categories Overview
print("📦 PRODUCT CATEGORIES OVERVIEW")
print("-"*70)

query2 = """
SELECT 
    product_category_name,
    COUNT(*) as product_count,
    ROUND(AVG(product_name_lenght), 1) as avg_name_length,
    ROUND(AVG(product_weight_g), 0) as avg_weight_grams
FROM dim_products
WHERE product_category_name IS NOT NULL
GROUP BY product_category_name
ORDER BY product_count DESC
LIMIT 10;
"""

result2 = pd.read_sql(query2, engine)
print(result2.to_string(index=False))
print()

result2 = pd.read_sql(query2, engine)
print(result2.to_string(index=False))
print()

# Query 3: Customers by City (Top 15)
print("🏙️  TOP 15 CITIES BY CUSTOMER COUNT")
print("-"*70)

query3 = """
SELECT 
    customer_city,
    customer_state,
    COUNT(*) as customer_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM dim_customers), 2) as pct_of_total
FROM dim_customers
GROUP BY customer_city, customer_state
ORDER BY customer_count DESC
LIMIT 15;
"""

result3 = pd.read_sql(query3, engine)
print(result3.to_string(index=False))
print()

# Query 4: Weekend vs Weekday Distribution
print("📅 WEEKEND VS WEEKDAY ANALYSIS")
print("-"*70)

query4 = """
SELECT 
    CASE 
        WHEN is_weekend THEN 'Weekend'
        ELSE 'Weekday'
    END as day_type,
    COUNT(*) as day_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM dim_date
GROUP BY is_weekend
ORDER BY day_count DESC;
"""

result4 = pd.read_sql(query4, engine)
print(result4.to_string(index=False))
print()

# Summary
print("="*70)
print("✨ ANALYSIS COMPLETE")
print("="*70)
print("\n📊 Key Insights:")
print("  - Customer base spans multiple Brazilian states")
print("  - Product catalog covers diverse categories")
print("  - Date dimension ready for time-based analysis")
print("\n💡 Next Steps:")
print("  - Add fact_orders table with order transactions")
print("  - Calculate revenue metrics")
print("  - Perform cohort analysis")
print("  - Build Tableau dashboard")

# Close connection
engine.dispose()