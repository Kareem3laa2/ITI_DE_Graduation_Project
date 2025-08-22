
  create or replace   view NYC_PROPERTIES.DWH_gold.stg_nyc_properties
  
   as (
    -- models/staging/stg_nyc_properties.sql


with deduped as (
    select
        address,
        cast(zip_code as varchar) as zip_code,
        neighborhood,
        block,
        lot,
        year_built,
        
        borough_name,
        
        tax_class_at_present,
        cast(tax_class_at_time_of_sale as varchar) as tax_class_at_time_of_sale,
        
        building_class_at_present,
        building_class_at_time_of_sale,
        building_class_category,
        
        sale_date,
        sale_price,
        
        residential_units,
        try_to_number(commercial_units) as commercial_units,
        total_units,
        
        land_square_feet,
        gross_square_feet,
        
        md5(cast(coalesce(cast(address as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(borough_name as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(block as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(lot as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as property_key,
        
        row_number() over (
            partition by
                address,
                borough_name,
                block,
                lot,
                zip_code,
                year_built,
                sale_date,
                sale_price
            order by sale_date desc
        ) as rn
    from NYC_PROPERTIES.STAGING.silver_nyc_properties
    where borough_name is not null
      and sale_date is not null
)

select *
from deduped
where rn = 1
  );

