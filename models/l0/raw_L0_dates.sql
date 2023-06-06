{{
    config(
        materialized='incremental',
        unique_key='date'
    )
}}

with staging_date as (

    select * from {{ ref('staging_dates') }}

),

final as (
    select

            staging_date.date_num as date_num,
            staging_date.Date as date,
            staging_date.YMN as YearMonthNum,
            staging_date.Calendar_Quarter as Calendar_Quarter,
            staging_date.month_num as month_num,
            staging_date.month_name as month_name,
            staging_date.Month_ShortName as Month_ShortName,
            staging_date.week_num as week_num,
            staging_date.D_Year as Day_Year,
            staging_date.D_month as Day_month,
            staging_date.D_week as Day_week,
            staging_date.D_name as Day_name,
            staging_date.D_shortname as Day_shortname,
            staging_date.Quarter as quarter,
            staging_date.YearQuarterNum as YearQuarterNum,
            staging_date.DayNumOfQuarter as DayNumOfQuarter

    from staging_date

)

select * from final

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where date > (select max(date) from {{ this }})

{% endif %}