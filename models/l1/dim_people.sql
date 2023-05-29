{{ config(materialized='table') }}

with people_data as (
    
    select * from {{ ref('raw_L0_orders') }} 
),
final as (

    select 
           people_data.person as person,
           people_data.people_region as people_region
    from 
     people_data
)

select * from final