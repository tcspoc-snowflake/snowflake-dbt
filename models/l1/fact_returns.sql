{{ config(materialized='table') }}

with retunrs as (
    
    select * from {{ ref('returns') }} 
),
final as (
    select
     returns.RETURNED as returned,
     returns.ORDERID as order_id
    from 
    returns
)

select * from final