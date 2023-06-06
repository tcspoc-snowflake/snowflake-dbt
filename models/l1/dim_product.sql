{{ config(materialized='table') }}

with products as (
    
    select * from {{ ref('raw_L0_orders') }} 
),
final as (

    select 
            products.PRODUCT_ID as product_id,
			products.CATEGORY as category,
			products.SUB_CATEGORY as sub_category,
			products.PRODUCT_NAME as product_name,
            products.CUSTOMER_ID as CUSTOMER_ID

            from 
            products

)

select * from final