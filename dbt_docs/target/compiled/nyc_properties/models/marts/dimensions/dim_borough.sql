-- models/marts/dimensions/dim_borough.sql


select
    row_number() over (order by borough_name) as borough_id,
    borough_name
from (
    select distinct 
        borough_name
    from NYC_PROPERTIES.DWH_gold.stg_nyc_properties
    where borough_name is not null
)
order by borough_id