with source as (
    select * from {{ source('raw', 'mappings_industries') }}
)
select
    industry_id,
    industry_name
from source
