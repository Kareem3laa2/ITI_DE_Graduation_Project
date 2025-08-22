select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select borough_name
from NYC_PROPERTIES.DWH_gold.dim_borough
where borough_name is null



      
    ) dbt_internal_test