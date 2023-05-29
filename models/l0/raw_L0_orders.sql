{{
    config(
        materialized='incremental',
        unique_key='order_id'
    )
}}

with staging_data as (

    select * from {{ ref('staging_orders') }}

),

final as (
    select

            staging_data.ROW_ID as row_id,
			staging_data.ORDER_ID as order_id,
			staging_data.ORDER_DATE as order_date,
			staging_data.Order_SHIPDATE as shipdate,
			staging_data.order_SHIPMODE as shipmode,
			staging_data.CUSTOMER_ID as customer_id,
			staging_data.CUSTOMER_NAME as customer_name,
			staging_data.SEGMENT as segment,
			staging_data.COUNTRY as country,
			staging_data.CITY as city,
			staging_data.o_STATE as state,
			staging_data.POSTALCODE as postalcode,
			staging_data.REGION as region,
			staging_data.PRODUCT_ID as product_id,
			staging_data.CATEGORY as category,
			staging_data.SUB_CATEGORY as sub_category,
			staging_data.PRODUCT_NAME as product_name,
			staging_data.SALES as sales,
			staging_data.QUANTITY as quantity,
			staging_data.DISCOUNT as discount,
			staging_data.PROFIT as profit,
            staging_data.person as person,
            staging_data.p_region as people_region

    from staging_data

)

select * from final

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where order_date > (select max(order_date) from {{ this }})

{% endif %}