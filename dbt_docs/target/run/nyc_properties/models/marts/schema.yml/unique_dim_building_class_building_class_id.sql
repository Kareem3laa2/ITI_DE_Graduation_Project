select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    building_class_id as unique_field,
    count(*) as n_records

from NYC_PROPERTIES.DWH_gold.dim_building_class
where building_class_id is not null
group by building_class_id
having count(*) > 1



      
    ) dbt_internal_test