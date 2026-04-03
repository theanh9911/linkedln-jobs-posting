with source as (
    select * from {{ source('raw', 'jobs_benefits') }}
)
select
    cast(job_id as string) as job_id,
    type as benefit_type
from source
