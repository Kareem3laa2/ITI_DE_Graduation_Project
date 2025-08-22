select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select building_class_at_present
from NYC_PROPERTIES.DWH_gold.dim_building_class
where building_class_at_present is null



      
    ) dbt_internal_test