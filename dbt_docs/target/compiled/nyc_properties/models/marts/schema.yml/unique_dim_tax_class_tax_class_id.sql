
    
    

select
    tax_class_id as unique_field,
    count(*) as n_records

from NYC_PROPERTIES.DWH_gold.dim_tax_class
where tax_class_id is not null
group by tax_class_id
having count(*) > 1


