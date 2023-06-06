{{
    config(
        materialized='table' )
}}

with fact_order as (

    select * from {{ ref('raw_L0_orders') }}

),

final as (
    select

            fact_order.ROW_ID as row_id,
			fact_order.ORDER_ID as order_id,
			fact_order.ORDER_DATE as order_date,
			fact_order.shipdate as shipdate,
			fact_order.shipmode as shipmode,
            fact_order.sales as sales,
            fact_order.Quantity as Quantity,
            fact_order.Discount as discount,
            fact_order.Profit as Profit,
            fact_order.product_id as product_id
            from 
            fact_order

)

select * from final