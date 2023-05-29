{{ config(materialized='table') }}

with dates as (
    
    select * from {{ ref('dimdates') }} 

),

final as (

    select 

			dates.DateNum as date_num,
            dates.Date as date,
            dates.YearMonthNum as YMN,
            dates.CalendarQuarter as Calendar_Quarter,
            dates.MonthNum as month_num,
            dates.MonthName as month_name,
            dates.MonthShortName as Month_ShortName,
            dates.WeekNum as week_num,
            dates.DayNumOfYear as D_Year,
            dates.DayNumOfMonth as D_month,
            dates.DayNumOfWeek as D_week,
            dates.DayName as D_name,
            dates.DayShortName as D_shortname,
            dates.Quarter as quarter,
            dates.YearQuarterNum as YearQuarterNum,
            dates.DayNumOfQuarter as DayNumOfQuarter

    from
        dates
      
)

select * from final 
