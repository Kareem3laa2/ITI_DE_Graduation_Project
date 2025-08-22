select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        borough_name as value_field,
        count(*) as n_records

    from NYC_PROPERTIES.DWH_gold.dim_borough
    group by borough_name

)

select *
from all_values
where value_field not in (
    'MANHATTAN','BROOKLYN','QUEENS','BRONX','STATEN ISLAND'
)



      
    ) dbt_internal_test