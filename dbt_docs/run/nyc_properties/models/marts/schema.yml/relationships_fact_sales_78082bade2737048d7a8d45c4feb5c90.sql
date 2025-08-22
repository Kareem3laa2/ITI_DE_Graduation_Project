select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with child as (
    select property_id as from_field
    from NYC_PROPERTIES.DWH_gold.fact_sales
    where property_id is not null
),

parent as (
    select property_id as to_field
    from NYC_PROPERTIES.DWH_gold.dim_property
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



      
    ) dbt_internal_test