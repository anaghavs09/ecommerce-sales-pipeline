-- Product-level performance metrics
-- Aggregated sales and performance data per product

{{
    config(
        materialized='table',
        tags=['marts', 'products', 'metrics']
    )
}}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

product_sales AS (
    SELECT 
        o.product_key,
        
        -- Sales volume
        COUNT(DISTINCT o.order_id) as times_ordered,
        SUM(o.order_item_id) as total_units_sold,
        
        -- Revenue metrics
        SUM(o.price) as total_revenue,
        ROUND(AVG(o.price)::NUMERIC, 2) as avg_price,
        MIN(o.price) as min_price,
        MAX(o.price) as max_price,
        
        -- Delivery performance
        ROUND(AVG(o.delivery_days)::NUMERIC, 1) as avg_delivery_days,
        
        -- Time metrics
        MIN(o.order_purchase_timestamp) as first_sale_date,
        MAX(o.order_purchase_timestamp) as last_sale_date
        
    FROM orders o
    GROUP BY o.product_key
),

final AS (
    SELECT 
        p.product_key,
        p.product_id,
        p.product_category_clean,
        p.product_category_name,
        p.weight_category,
        p.size_category,
        p.product_weight_g,
        p.product_volume_cm3,
        
        -- Sales metrics
        COALESCE(s.times_ordered, 0) as times_ordered,
        COALESCE(s.total_units_sold, 0) as total_units_sold,
        COALESCE(s.total_revenue, 0) as total_revenue,
        s.avg_price,
        s.min_price,
        s.max_price,
        s.avg_delivery_days,
        s.first_sale_date,
        s.last_sale_date,
        
        -- Performance category
        CASE 
            WHEN COALESCE(s.times_ordered, 0) >= 100 THEN 'Best Seller'
            WHEN COALESCE(s.times_ordered, 0) >= 50 THEN 'Popular'
            WHEN COALESCE(s.times_ordered, 0) >= 10 THEN 'Regular'
            WHEN COALESCE(s.times_ordered, 0) > 0 THEN 'Slow Moving'
            ELSE 'Never Sold'
        END as performance_category,
        
        -- Metadata
        CURRENT_TIMESTAMP as dbt_updated_at
        
    FROM products p
    LEFT JOIN product_sales s ON p.product_key = s.product_key
)

SELECT * FROM final