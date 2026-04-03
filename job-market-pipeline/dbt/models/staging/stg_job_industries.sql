with source as (
    select * from {{ source('raw', 'jobs_job_industries') }}
)
select
    cast(job_id as string) as job_id,
    industry_id
from source
