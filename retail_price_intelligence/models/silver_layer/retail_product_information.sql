{{ config(materialized='table') }}

with source_data as (

    select name,
    price_ron,
    url,
    scrape_date
    from {{ ref('retail_information') }}

),

cleaned_data as (

    select
    {{ dbt_utils.generate_surrogate_key([
				'name', 
				'scrape_date'
			])
		}} as unique_key, 
        name,
        price_ron::numeric/100 as price_ron,
        url,
        scrape_date
    from source_data

)

select *
from cleaned_data