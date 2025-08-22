select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select sale_date_id
from NYC_PROPERTIES.DWH_gold.fact_sales
where sale_date_id is null



      
    ) dbt_internal_test