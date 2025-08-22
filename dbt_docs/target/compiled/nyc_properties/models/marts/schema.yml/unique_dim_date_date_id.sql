
    
    

select
    date_id as unique_field,
    count(*) as n_records

from NYC_PROPERTIES.DWH_gold.dim_date
where date_id is not null
group by date_id
having count(*) > 1


