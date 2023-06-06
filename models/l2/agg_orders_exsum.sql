{{ config(materialized='table') }}

with D as (
    
    select * from {{ ref('dim_customer') }} 

),

E as  (

    select * from {{ ref('dim_product')}}
),

B as (

    select * from {{ ref('fact_orders')}}
),

A as (

    select * from {{ ref('fact_returns')}}
),

C as (

    select * from {{ ref('dim_date')}}
),

P as (

    select * from {{ ref('dim_people')}}
),

final as (

    select 
        C.YearMonthNum,
        C.month_name,
        C.Calendar_Quarter,
        count(D.CUSTOMER_ID) as total_customers,
        P.person as person,   ---added by ram 
        D.REGION as region,
		D.COUNTRY as country,
		E.CATEGORY as category,
		count(B.ORDER_ID) as total_orders,
        count( A.order_id) as total_returned_orders,
        sum(b.sales) as total_sales,
        sum(b.Quantity) as total_Quantity,
        sum(b.Discount) as total_discount,
        sum(b.Profit) as total_Profit,
		sum(case when a.returned = 'Yes' then b.Sales else 0 end) as returned_sales,
        sum(case when a.returned = 'Yes' then b.Quantity else 0 end) as returned_Quantity,
        sum(case when a.returned = 'Yes' then b.Discount else 0 end) as returned_Discount,
        sum(case when a.returned = 'Yes' then b.Profit else 0 end) as returned_Profit
    
      from dim_people P
      inner join dim_customer D on P.people_region=D.REGION
      inner join dim_product E on D.CUSTOMER_ID=E.CUSTOMER_ID
      inner join fact_orders B on B.product_id=E.product_id
      right outer join FACT_RETURNS A on A.order_id=B.order_id
      inner join dim_date C on C.date=B.ORDER_DATE

      Group By 
     
		C.YearMonthNum,
        C.month_name,
        C.Calendar_Quarter,
        D.REGION,
		D.COUNTRY,
		E.CATEGORY,
         P.person
)

select * from final 
