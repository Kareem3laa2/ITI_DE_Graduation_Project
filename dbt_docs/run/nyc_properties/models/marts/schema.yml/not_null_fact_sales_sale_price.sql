select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select sale_price
from NYC_PROPERTIES.DWH_gold.fact_sales
where sale_price is null



      
    ) dbt_internal_test