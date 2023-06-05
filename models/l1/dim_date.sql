{{
    config(
        materialized='table'
    )
}}

with dim_date as (

    select * from {{ ref('raw_L0_dates') }}

),

final as (
    select

            dim_date.date_num as date_num,
            dim_date.Date as date,
            dim_date.YearMonthNum as YearMonthNum,
            dim_date.Calendar_Quarter as Calendar_Quarter,
            dim_date.month_num as month_num,
            dim_date.month_name as month_name,
            dim_date.Month_ShortName as Month_ShortName,
            dim_date.week_num as week_num,
            dim_date.Day_Year as Day_Year,
            dim_date.Day_month as Day_month,
            dim_date.Day_week as Day_week,
            dim_date.Day_name as Day_name,
            dim_date.Day_shortname as Day_shortname,
            dim_date.Quarter as quarter,
            dim_date.YearQuarterNum as YearQuarterNum,
            dim_date.DayNumOfQuarter as DayNumOfQuarter

    from dim_date

)

select * from final

