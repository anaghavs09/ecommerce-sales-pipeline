-- Staging model for products
-- Cleans and categorizes product dimension

{{
    config(
        materialized='table',
        tags=['staging', 'products']
    )
}}

WITH source AS (
    SELECT * FROM {{ source('public', 'dim_products') }}
),

enhanced AS (
    SELECT 
        product_key,
        product_id,
        
        -- Clean category names
        INITCAP(REPLACE(product_category_name, '_', ' ')) as product_category_clean,
        product_category_name,
        
        -- Product attributes
        product_name_lenght,
        product_description_lenght,
        product_photos_qty,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm,
        
        -- Calculate volume
        (product_length_cm * product_height_cm * product_width_cm) as product_volume_cm3,
        
        -- Weight category
        CASE 
            WHEN product_weight_g < 500 THEN 'Light'
            WHEN product_weight_g < 2000 THEN 'Medium'
            WHEN product_weight_g < 10000 THEN 'Heavy'
            ELSE 'Very Heavy'
        END as weight_category,
        
        -- Size category
        CASE 
            WHEN (product_length_cm * product_height_cm * product_width_cm) < 1000 THEN 'Small'
            WHEN (product_length_cm * product_height_cm * product_width_cm) < 10000 THEN 'Medium'
            ELSE 'Large'
        END as size_category,
        
        -- Metadata
        CURRENT_TIMESTAMP as dbt_updated_at
        
    FROM source
    WHERE product_category_name IS NOT NULL
)

SELECT * FROM enhanced