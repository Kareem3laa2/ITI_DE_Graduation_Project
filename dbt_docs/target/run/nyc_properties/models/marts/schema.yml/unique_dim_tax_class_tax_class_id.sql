select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    tax_class_id as unique_field,
    count(*) as n_records

from NYC_PROPERTIES.DWH_gold.dim_tax_class
where tax_class_id is not null
group by tax_class_id
having count(*) > 1



      
    ) dbt_internal_test