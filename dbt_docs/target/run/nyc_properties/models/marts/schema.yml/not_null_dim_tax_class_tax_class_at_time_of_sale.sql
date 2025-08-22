select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select tax_class_at_time_of_sale
from NYC_PROPERTIES.DWH_gold.dim_tax_class
where tax_class_at_time_of_sale is null



      
    ) dbt_internal_test