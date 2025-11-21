-- Staging model for customers
-- Cleans and enhances customer dimension

{{
    config(
        materialized='table',
        tags=['staging', 'customers']
    )
}}

WITH source AS (
    SELECT * FROM {{ source('public', 'dim_customers') }}
),

enhanced AS (
    SELECT 
        customer_key,
        customer_id,
        
        -- Clean text formatting
        INITCAP(customer_city) as customer_city,
        UPPER(customer_state) as customer_state,
        customer_zip_code_prefix,
        
        -- Add business flags
        CASE 
            WHEN customer_state = 'sp' THEN true 
            ELSE false 
        END as is_sao_paulo,
        
        CASE 
            WHEN customer_state IN ('sp', 'rj', 'mg') THEN true
            ELSE false
        END as is_top_3_state,
        
        -- Metadata
        CURRENT_TIMESTAMP as dbt_updated_at
        
    FROM source
)

SELECT * FROM enhanced