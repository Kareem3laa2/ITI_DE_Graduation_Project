-- models/marts/dimensions/dim_date.sql


with date_spine as (
    





with rawdata as (

    

    

    with p as (
        select 0 as generated_number union all select 1
    ), unioned as (

    select

    
    p0.generated_number * power(2, 0)
     + 
    
    p1.generated_number * power(2, 1)
     + 
    
    p2.generated_number * power(2, 2)
     + 
    
    p3.generated_number * power(2, 3)
     + 
    
    p4.generated_number * power(2, 4)
     + 
    
    p5.generated_number * power(2, 5)
     + 
    
    p6.generated_number * power(2, 6)
     + 
    
    p7.generated_number * power(2, 7)
     + 
    
    p8.generated_number * power(2, 8)
     + 
    
    p9.generated_number * power(2, 9)
     + 
    
    p10.generated_number * power(2, 10)
     + 
    
    p11.generated_number * power(2, 11)
     + 
    
    p12.generated_number * power(2, 12)
    
    
    + 1
    as generated_number

    from

    
    p as p0
     cross join 
    
    p as p1
     cross join 
    
    p as p2
     cross join 
    
    p as p3
     cross join 
    
    p as p4
     cross join 
    
    p as p5
     cross join 
    
    p as p6
     cross join 
    
    p as p7
     cross join 
    
    p as p8
     cross join 
    
    p as p9
     cross join 
    
    p as p10
     cross join 
    
    p as p11
     cross join 
    
    p as p12
    
    

    )

    select *
    from unioned
    where generated_number <= 5843
    order by generated_number



),

all_periods as (

    select (
        

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        cast('2015-01-01' as date)
        )


    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= cast('2030-12-31' as date)

)

select * from filtered


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