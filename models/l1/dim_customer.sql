{{ config(materialized='table') }}

with customers as (
    
    select * from {{ ref('raw_L0_orders') }} 
),
final as (

    select 
            customers.CUSTOMER_ID as customer_id,
			customers.CUSTOMER_NAME as customer_name,
			customers.SEGMENT as segment,
			customers.COUNTRY as country,
			customers.CITY as city,
			customers.STATE as o_state,
			customers.POSTALCODE as postalcode,
			customers.REGION as region

    from 
     customers
)

select * from final