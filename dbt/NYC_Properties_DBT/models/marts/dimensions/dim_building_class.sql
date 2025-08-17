-- models/marts/dimensions/dim_building_class.sql
{{ config(
    materialized='table',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (building_class_id)"
) }}

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
    from {{ ref('stg_nyc_properties') }}
    where building_class_at_time_of_sale is not null
)
order by building_class_id
