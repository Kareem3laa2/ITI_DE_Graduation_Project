
  
    

        create or replace transient table NYC_PROPERTIES.DWH_gold.dim_building_class
         as
        (-- models/marts/dimensions/dim_building_class.sql


select
    row_number() over (
        order by building_class_at_time_of_sale, building_class_at_present, building_class_category
    ) as building_class_id,
    building_class_at_time_of_sale,
    building_class_at_present,
    building_class_category
from (
    select distinct
        building_class_at_time_of_sale,
        building_class_at_present,
        building_class_category
    from NYC_PROPERTIES.DWH_gold.stg_nyc_properties
    where building_class_at_time_of_sale is not null
)
order by building_class_id
        );
      
  