
  
    

        create or replace transient table NYC_PROPERTIES.DWH_gold.fact_sales
         as
        (-- models/marts/facts/fact_sales.sql


with joined_sales as (
    select
        d.date_id as sale_date_id,
        p.property_id,
        bc.building_class_id,
        tc.tax_class_id,
        s.sale_price,
        s.residential_units,
        s.commercial_units,
        s.total_units,
        s.land_square_feet,
        s.gross_square_feet,
        row_number() over (
            partition by 
                d.date_id,
                p.property_id,
                bc.building_class_id,
                tc.tax_class_id,
                s.sale_price
            order by s.sale_date desc
        ) as rn
    from NYC_PROPERTIES.DWH_gold.stg_nyc_properties s
    join NYC_PROPERTIES.DWH_gold.dim_date d
        on cast(s.sale_date as date) = d.full_date
    join NYC_PROPERTIES.DWH_gold.dim_property p
        on trim(lower(s.address)) = trim(lower(p.address))
       and s.zip_code = p.zip_code
    join NYC_PROPERTIES.DWH_gold.dim_building_class bc
        on s.building_class_at_time_of_sale = bc.building_class_at_time_of_sale
       and s.building_class_at_present = bc.building_class_at_present
       and s.building_class_category = bc.building_class_category
    join NYC_PROPERTIES.DWH_gold.dim_tax_class tc
        on s.tax_class_at_time_of_sale = tc.tax_class_at_time_of_sale
       and s.tax_class_at_present = tc.tax_class_at_present
    where s.sale_date is not null
      and s.borough_name is not null
)

select
    row_number() over (order by sale_date_id, property_id, building_class_id, tax_class_id, sale_price) as fact_id,
    sale_date_id,
    property_id,
    building_class_id,
    tax_class_id,
    sale_price,
    residential_units,
    commercial_units,
    total_units,
    land_square_feet,
    gross_square_feet
from joined_sales
where rn = 1
order by fact_id
        );
      
  