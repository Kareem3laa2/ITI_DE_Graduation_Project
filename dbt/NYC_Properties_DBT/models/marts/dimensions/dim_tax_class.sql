-- models/marts/dimensions/dim_tax_class.sql
{{ config(
    materialized='table',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (tax_class_id)"
) }}

select
    row_number() over (
        order by tax_class_at_present, tax_class_at_time_of_sale
    ) as tax_class_id,
    tax_class_at_present,
    tax_class_at_time_of_sale
from (
    select distinct
        tax_class_at_present,
        tax_class_at_time_of_sale
    from {{ ref('stg_nyc_properties') }}
    where tax_class_at_present is not null
)
order by tax_class_id
