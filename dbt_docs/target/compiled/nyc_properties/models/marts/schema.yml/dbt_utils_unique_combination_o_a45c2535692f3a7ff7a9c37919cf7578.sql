





with validation_errors as (

    select
        sale_date_id, property_id, building_class_id, tax_class_id, sale_price
    from NYC_PROPERTIES.DWH_gold.fact_sales
    group by sale_date_id, property_id, building_class_id, tax_class_id, sale_price
    having count(*) > 1

)

select *
from validation_errors


