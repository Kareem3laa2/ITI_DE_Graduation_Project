
    
    

with all_values as (

    select
        year as value_field,
        count(*) as n_records

    from NYC_PROPERTIES.DWH_gold.dim_date
    group by year

)

select *
from all_values
where value_field not in (
    '2015','2016','2017','2018','2019','2020','2021','2022','2023','2024','2025','2026','2027','2028','2029','2030'
)


