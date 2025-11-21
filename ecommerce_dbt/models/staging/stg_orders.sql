-- Staging model for orders
-- Enhances fact table with calculated metrics

{{
    config(
        materialized='table',
        tags=['staging', 'orders']
    )
}}

WITH source AS (
    SELECT * FROM {{ source('public', 'fact_orders') }}
),

enhanced AS (
    SELECT 
        order_key,
        order_id,
        customer_key,
        product_key,
        order_date_key,
        
        -- Metrics
        order_item_id,
        price,
        freight_value,
        price + freight_value as total_amount,
        
        -- Attributes
        order_status,
        seller_id,
        
        -- Timestamps
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date,
        
        -- Calculate delivery time (in days)
        CASE 
            WHEN order_delivered_customer_date IS NOT NULL 
            THEN DATE_PART('day', order_delivered_customer_date - order_purchase_timestamp)
        END as delivery_days,
        
        -- Early or late delivery
        CASE 
            WHEN order_delivered_customer_date IS NOT NULL 
            THEN CASE 
                WHEN order_delivered_customer_date <= order_estimated_delivery_date THEN 'On Time'
                ELSE 'Late'
            END
        END as delivery_status,
        
        -- Approval time (in hours)
        CASE 
            WHEN order_approved_at IS NOT NULL
            THEN ROUND(EXTRACT(EPOCH FROM (order_approved_at - order_purchase_timestamp)) / 3600, 2)
        END as approval_hours,
        
        -- Metadata
        CURRENT_TIMESTAMP as dbt_updated_at
        
    FROM source
    WHERE order_status = 'delivered'  -- Only analyze delivered orders
)

SELECT * FROM enhanced