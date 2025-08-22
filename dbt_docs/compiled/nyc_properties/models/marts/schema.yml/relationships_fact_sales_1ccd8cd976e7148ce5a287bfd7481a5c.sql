
    
    

with child as (
    select tax_class_id as from_field
    from NYC_PROPERTIES.DWH_gold.fact_sales
    where tax_class_id is not null
),

parent as (
    select tax_class_id as to_field
    from NYC_PROPERTIES.DWH_gold.dim_tax_class
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


