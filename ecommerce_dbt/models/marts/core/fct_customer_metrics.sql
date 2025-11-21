-- Customer-level aggregated metrics
-- One row per customer with lifetime statistics

{{
    config(
        materialized='table',
        tags=['marts', 'customers', 'metrics']
    )
}}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

customer_metrics AS (
    SELECT 
        o.customer_key,
        
        -- Order counts
        COUNT(DISTINCT o.order_id) as total_orders,
        COUNT(*) as total_items_purchased,
        
        -- Revenue metrics
        SUM(o.price) as total_revenue,
        SUM(o.total_amount) as total_amount_with_freight,
        ROUND(AVG(o.price)::NUMERIC, 2) as avg_order_value,
        ROUND(AVG(o.total_amount)::NUMERIC, 2) as avg_total_with_freight,
        
        -- Shipping metrics
        SUM(o.freight_value) as total_freight_paid,
        ROUND(AVG(o.freight_value)::NUMERIC, 2) as avg_freight_per_order,
        
        -- Delivery performance
        ROUND(AVG(o.delivery_days)::NUMERIC, 1) as avg_delivery_days,
        SUM(CASE WHEN o.delivery_status = 'On Time' THEN 1 ELSE 0 END) as on_time_deliveries,
        SUM(CASE WHEN o.delivery_status = 'Late' THEN 1 ELSE 0 END) as late_deliveries,
        
        -- Time metrics
        MIN(o.order_purchase_timestamp) as first_order_date,
        MAX(o.order_purchase_timestamp) as last_order_date,
        
        -- Calculate customer lifetime (in days)
        DATE_PART('day', MAX(o.order_purchase_timestamp) - MIN(o.order_purchase_timestamp)) as customer_lifetime_days,
        
        -- Customer segmentation
        CASE 
            WHEN COUNT(DISTINCT o.order_id) = 1 THEN 'One-time'
            WHEN COUNT(DISTINCT o.order_id) BETWEEN 2 AND 3 THEN 'Occasional'
            WHEN COUNT(DISTINCT o.order_id) >= 4 THEN 'Frequent'
        END as customer_segment
        
    FROM orders o
    GROUP BY o.customer_key
),

final AS (
    SELECT 
        c.customer_key,
        c.customer_id,
        c.customer_city,
        c.customer_state,
        c.is_sao_paulo,
        c.is_top_3_state,
        
        -- Metrics from aggregation
        m.total_orders,
        m.total_items_purchased,
        m.total_revenue,
        m.total_amount_with_freight,
        m.avg_order_value,
        m.avg_total_with_freight,
        m.total_freight_paid,
        m.avg_freight_per_order,
        m.avg_delivery_days,
        m.on_time_deliveries,
        m.late_deliveries,
        m.first_order_date,
        m.last_order_date,
        m.customer_lifetime_days,
        m.customer_segment,
        
        -- Calculate on-time percentage
        ROUND(
            (m.on_time_deliveries::NUMERIC / NULLIF(m.total_orders, 0)) * 100, 
            1
        ) as on_time_pct,
        
        -- Metadata
        CURRENT_TIMESTAMP as dbt_updated_at
        
    FROM customers c
    INNER JOIN customer_metrics m ON c.customer_key = m.customer_key
)

SELECT * FROM final