{{ config(materialized="table") }}

with
    customers as (select * from {{ ref("raw_L0_orders") }}),

    ext_customers as (

        select
            id as customer_id,
            name as customer_name,
            company as segment,
            'United States' as country,
            city as city,
            state as o_state,
            postcode as postalcode,
            'North' as region
        from {{ source("delta_tables", "ext_customer") }}
    ),

    final as (

        select distinct
            customers.customer_id as customer_id,
            customers.customer_name as customer_name,
            customers.segment as segment,
            customers.country as country,
            customers.city as city,
            customers.state as o_state,
            customers.postalcode as postalcode,
            customers.region as region

        from customers

        union

        select
            ext_customers.customer_id as customer_id,
            ext_customers.customer_name as customer_name,
            ext_customers.segment as segment,
            ext_customers.country as country,
            ext_customers.city as city,
            ext_customers.o_state as o_state,
            ext_customers.postalcode as postalcode,
            ext_customers.region as region

        from ext_customers
    )

select *
from final
