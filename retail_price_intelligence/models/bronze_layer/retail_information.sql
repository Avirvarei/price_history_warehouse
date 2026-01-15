{{ config(materialized='table') }}

with source_data as (

    select name,
    price_ron,
    url,
    scrape_date
    from {{ source('raw_data', 'emag_phones') }}

)

select *
from source_data

