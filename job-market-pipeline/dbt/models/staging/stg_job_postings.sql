with source as (
    select * from {{ source('raw', 'postings') }}
)

select
    -- Khóa (Keys)
    cast(job_id as string) as job_id,
    cast(company_id as string) as company_id,
    
    -- Thông tin cơ bản (Core info)
    title,
    description,
    location,
    remote_allowed,
    
    -- Lương (Salary)
    max_salary,
    med_salary,
    min_salary,
    pay_period,
    currency,
    compensation_type,
    
    -- Phân loại (Classification)
    work_type,
    formatted_work_type,
    formatted_experience_level as experience_level,
    
    -- Chỉ số (Metrics)
    applies,
    views,
    
    -- Thời gian (Time)
    cast(posted_date as date) as posted_date,
    listed_time,
    expiry,
    closed_time

from source
