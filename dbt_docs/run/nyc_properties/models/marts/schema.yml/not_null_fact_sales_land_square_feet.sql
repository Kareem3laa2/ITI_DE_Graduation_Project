select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select land_square_feet
from NYC_PROPERTIES.DWH_gold.fact_sales
where land_square_feet is null



      
    ) dbt_internal_test