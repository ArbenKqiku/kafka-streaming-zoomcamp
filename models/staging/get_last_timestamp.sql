{{
    config(
        materialized='table'
    )
}}

select
    published_at
from
    {{ source('staging', 'company_house_stream')}}
order by
    published_at desc
limit
    1