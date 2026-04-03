with source as (
    select * from {{ source('raw', 'jobs_salaries') }}
)
select
    cast(job_id as string) as job_id,
    max_salary,
    med_salary,
    min_salary,
    pay_period,
    currency,
    compensation_type
from source
