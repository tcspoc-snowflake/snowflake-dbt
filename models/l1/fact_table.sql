{{ config(materialized='table') }}

with fact as (
    
    select * from {{ ref('orders') }} 
),
retunrs_data as (
    select * from {{ ref('returns')}}

),
final as (
    select 
    fact.ORDERID as order_id,
    fact.CUSTOMERID as customer_id,
    fact.PRODUCTID as product_id,
    retunrs_data.RETURNED as RETURNED
    from 
    fact 
    inner join retunrs_data
    on fact.orderid=retunrs_data.orderid
)

select * from final