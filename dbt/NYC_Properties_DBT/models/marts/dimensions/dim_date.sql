-- models/marts/dimensions/dim_date.sql
{{ config(
    materialized='table',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (date_id)"
) }}

with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2015-01-01' as date)",
        end_date="cast('2030-12-31' as date)"
    ) }}
),

date_details as (
    select
        date_day as full_date,
        to_number(to_char(date_day, 'YYYYMMDD')) as date_id,
        extract(day from date_day) as day,
        extract(month from date_day) as month,
        to_char(date_day, 'MMMM') as month_name,
        extract(month from date_day) as month_sort_order,  -- Add this for sorting
        cast(extract(year from date_day) as int) as year,
        extract(quarter from date_day) as quarter,             
        case 
            when dayofweek(date_day) in (1, 7) then true 
            else false 
        end as weekend_flag
    from date_spine
)

select
    date_id,
    full_date,
    day,
    month,
    month_name,
    month_sort_order,
    year,
    quarter,
    weekend_flag
from date_details
order by date_id