{% set start_date = var('cal_start_date') %}
{% set end_date = var('cal_end_date') %}

with numbers as (
  select top (DATEDIFF(day, '{{ start_date }}', '{{ end_date }}') + 1)
         row_number() over (order by (select null)) - 1 as n
  from sys.all_objects
),
dates as (
  select dateadd(day, n, cast('{{ start_date }}' as date)) as date_value
  from numbers
),

dim_date as (

    select
        cast(format(date_value, 'yyyyMMdd') as int) as date_key,
        date_value as full_date,
        datepart(weekday, date_value) + 1 as day_of_week,
        datename(weekday, date_value) as day_name,
        case datename(weekday, date_value)
            when 'Monday' then 'Maandag'
            when 'Tuesday' then 'Dinsdag'
            when 'Wednesday' then 'Woensdag'
            when 'Thursday' then 'Donderdag'
            when 'Friday' then 'Vrydag'
            when 'Saturday' then 'Saterdag'
            when 'Sunday' then 'Sondag'
        end as dag_naam,
        day(date_value) as day_of_month,
        datepart(dayofyear, date_value) as day_of_year,
        datepart(week, date_value) as week_of_year,
        month(date_value) as month,
        datename(month, date_value) as month_name,
        case datename(month, date_value)
            when 'January' then 'Januarie'
            when 'February' then 'Februarie'
            when 'March' then 'Maart'
            when 'April' then 'April'
            when 'May' then 'Mei'
            when 'June' then 'Junie'
            when 'July' then 'Julie'
            when 'August' then 'Augustus'
            when 'September' then 'September'
            when 'October' then 'Oktober'
            when 'November' then 'November'
            when 'December' then 'Desember'
        end as maand_naam,
        format(date_value, 'MMM-yy') as month_year,
        datepart(quarter, date_value) as quarter,
        year(date_value) as year,
        case when datepart(weekday, date_value) in (1, 7) then 1 else 0 end as is_weekend,
        0 as is_holiday, -- you can update later via post-hook or manual process
        case
            when month(date_value) in (12, 1, 2) then 'Summer'
            when month(date_value) in (3, 4, 5) then 'Autumn'
            when month(date_value) in (6, 7, 8) then 'Winter'
            when month(date_value) in (9, 10, 11) then 'Spring'
        end as season,
        getdate() as insert_date,
        getdate() as update_date

    from dates

)

select * from dim_date