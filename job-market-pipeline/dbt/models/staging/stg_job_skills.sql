with source as (
    select * from {{ source('raw', 'jobs_job_skills') }}
)
select
    cast(job_id as string) as job_id,
    skill_abr
from source
