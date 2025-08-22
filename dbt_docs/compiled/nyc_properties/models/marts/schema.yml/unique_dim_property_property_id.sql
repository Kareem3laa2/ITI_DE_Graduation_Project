
    
    

select
    property_id as unique_field,
    count(*) as n_records

from NYC_PROPERTIES.DWH_gold.dim_property
where property_id is not null
group by property_id
having count(*) > 1


