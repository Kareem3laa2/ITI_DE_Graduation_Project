

with unique_properties as (
    select distinct
        trim(lower(address)) as address,
        cast(zip_code as varchar) as zip_code,
        borough_name,
        neighborhood,
        block,
        lot,
        year_built
    from NYC_PROPERTIES.DWH_gold.stg_nyc_properties
    where borough_name is not null
)

select
    row_number() over (
        order by up.address, up.borough_name, up.block, up.lot, up.zip_code, up.year_built
    ) as property_id,
    up.address,
    up.zip_code,
    b.borough_id,
    up.neighborhood,
    up.block,
    up.lot,
    up.year_built
from unique_properties up
join NYC_PROPERTIES.DWH_gold.dim_borough b
    on trim(lower(up.borough_name)) = trim(lower(b.borough_name))
order by property_id