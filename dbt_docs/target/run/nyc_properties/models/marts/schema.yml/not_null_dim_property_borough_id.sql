select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select borough_id
from NYC_PROPERTIES.DWH_gold.dim_property
where borough_id is null



      
    ) dbt_internal_test