
    
    

with child as (
    select borough_id as from_field
    from NYC_PROPERTIES.DWH_gold.dim_property
    where borough_id is not null
),

parent as (
    select borough_id as to_field
    from NYC_PROPERTIES.DWH_gold.dim_borough
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


