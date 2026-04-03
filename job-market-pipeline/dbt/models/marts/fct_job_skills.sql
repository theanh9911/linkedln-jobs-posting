with job_skills as (
    select * from {{ ref('stg_job_skills') }}
)
select
    job_id,
    skill_abr as skill_id
from job_skills
