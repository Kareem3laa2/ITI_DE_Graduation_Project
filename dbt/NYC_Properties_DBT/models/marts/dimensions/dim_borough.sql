-- models/marts/dimensions/dim_borough.sql
{{ config(
    materialized='table',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (borough_id)"
) }}

select
    row_number() over (order by borough_name) as borough_id,
    borough_name
from (
    select distinct 
        borough_name
    from {{ ref('stg_nyc_properties') }}
    where borough_name is not null
)
order by borough_id
