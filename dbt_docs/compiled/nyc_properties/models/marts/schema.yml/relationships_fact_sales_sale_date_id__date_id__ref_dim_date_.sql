
    
    

with child as (
    select sale_date_id as from_field
    from NYC_PROPERTIES.DWH_gold.fact_sales
    where sale_date_id is not null
),

parent as (
    select date_id as to_field
    from NYC_PROPERTIES.DWH_gold.dim_date
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


