-- ============================================
-- E-COMMERCE DATA WAREHOUSE SCHEMA
-- ============================================
-- Star Schema Design:
-- - 3 Dimension Tables: customers, products, date
-- - 1 Fact Table: orders (transactions)
--
-- ============================================
-- Drop tables if they exist (clean slate)
DROP TABLE IF EXISTS fact_orders CASCADE;
DROP TABLE IF EXISTS dim_customers CASCADE;
DROP TABLE IF EXISTS dim_products CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;

-- ============================================
-- DIMENSION TABLE 1: Customers
-- ============================================
CREATE TABLE dim_customers (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(100) UNIQUE NOT NULL,
    customer_city VARCHAR(100),
    customer_state VARCHAR(50),
    customer_zip_code_prefix INTEGER
);

-- ============================================
-- DIMENSION TABLE 2: Products
-- ============================================
CREATE TABLE dim_products (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(100) UNIQUE NOT NULL,
    product_category_name VARCHAR(100),
    product_name_lenght INTEGER,
    product_description_lenght INTEGER,
    product_photos_qty INTEGER,
    product_weight_g INTEGER,
    product_length_cm INTEGER,
    product_height_cm INTEGER,
    product_width_cm INTEGER
);

-- ============================================
-- DIMENSION TABLE 3: Date (Calendar Table)
-- ============================================
CREATE TABLE dim_date (
    date_key SERIAL PRIMARY KEY,
    date DATE UNIQUE NOT NULL,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    month_name VARCHAR(20),
    day INTEGER,
    day_of_week INTEGER,
    day_name VARCHAR(20),
    week_of_year INTEGER,
    is_weekend BOOLEAN
);

-- ============================================
-- FACT TABLE: Orders (Transactions)
-- ============================================
CREATE TABLE fact_orders (
    order_key SERIAL PRIMARY KEY,
    order_id VARCHAR(100) NOT NULL,
    
    -- Foreign keys to dimensions
    customer_key INTEGER REFERENCES dim_customers(customer_key),
    product_key INTEGER REFERENCES dim_products(product_key),
    order_date_key INTEGER REFERENCES dim_date(date_key),
    
    -- Metrics (numbers we analyze)
    order_item_id INTEGER,
    price DECIMAL(10, 2),
    freight_value DECIMAL(10, 2),
    quantity INTEGER,
    
    -- Attributes
    order_status VARCHAR(50),
    seller_id VARCHAR(100),
    
    -- Timestamps
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
);

-- ============================================
-- INDEXES (Speed up queries!)
-- ============================================

-- Fact table indexes (frequently queried columns)
CREATE INDEX idx_fact_customer ON fact_orders(customer_key);
CREATE INDEX idx_fact_product ON fact_orders(product_key);
CREATE INDEX idx_fact_date ON fact_orders(order_date_key);
CREATE INDEX idx_fact_status ON fact_orders(order_status);

-- Dimension indexes
CREATE INDEX idx_dim_customer_id ON dim_customers(customer_id);
CREATE INDEX idx_dim_product_id ON dim_products(product_id);
CREATE INDEX idx_dim_date_date ON dim_date(date);