with source as (
    select * from {{ source('raw', 'mappings_skills') }}
)
select
    skill_abr,
    skill_name
from source
