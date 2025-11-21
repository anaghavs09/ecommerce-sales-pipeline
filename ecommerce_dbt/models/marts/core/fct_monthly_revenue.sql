-- Monthly revenue aggregations
-- Time-series analysis of sales performance

{{
    config(
        materialized='table',
        tags=['marts', 'revenue', 'time-series']
    )
}}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

dates AS (
    SELECT * FROM {{ source('public', 'dim_date') }}
),

monthly_stats AS (
    SELECT 
        d.year,
        d.month,
        d.month_name,
        
        -- Order metrics
        COUNT(DISTINCT o.order_id) as total_orders,
        COUNT(*) as total_items,
        COUNT(DISTINCT o.customer_key) as unique_customers,
        
        -- Revenue metrics
        SUM(o.price) as total_revenue,
        SUM(o.total_amount) as total_with_freight,
        ROUND(AVG(o.price)::NUMERIC, 2) as avg_order_value,
        
        -- Delivery metrics
        ROUND(AVG(o.delivery_days)::NUMERIC, 1) as avg_delivery_days,
        SUM(CASE WHEN o.delivery_status = 'On Time' THEN 1 ELSE 0 END) as on_time_count,
        SUM(CASE WHEN o.delivery_status = 'Late' THEN 1 ELSE 0 END) as late_count
        
    FROM orders o
    INNER JOIN dates d ON o.order_date_key = d.date_key
    GROUP BY d.year, d.month, d.month_name
),

final AS (
    SELECT 
        year,
        month,
        month_name,
        
        -- Create date for sorting
        DATE(year || '-' || LPAD(month::TEXT, 2, '0') || '-01') as month_date,
        
        total_orders,
        total_items,
        unique_customers,
        total_revenue,
        total_with_freight,
        avg_order_value,
        avg_delivery_days,
        on_time_count,
        late_count,
        
        -- Calculate on-time percentage
        ROUND((on_time_count::NUMERIC / NULLIF(total_orders, 0)) * 100, 1) as on_time_pct,
        
        -- Calculate growth vs previous month (window function!)
        LAG(total_revenue) OVER (ORDER BY year, month) as prev_month_revenue,
        ROUND(
            ((total_revenue - LAG(total_revenue) OVER (ORDER BY year, month))::NUMERIC 
            / NULLIF(LAG(total_revenue) OVER (ORDER BY year, month), 0)) * 100,
            1
        ) as revenue_growth_pct,
        
        -- Metadata
        CURRENT_TIMESTAMP as dbt_updated_at
        
    FROM monthly_stats
)

SELECT * FROM final
ORDER BY year, month