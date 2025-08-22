select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select borough_name
from NYC_PROPERTIES.STAGING.silver_nyc_properties
where borough_name is null



      
    ) dbt_internal_test