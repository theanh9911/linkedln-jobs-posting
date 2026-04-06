with source as (
    select * from {{ source('raw', 'postings') }}
),

typed as (
    select
        *,
        trim(coalesce(cast(formatted_work_type as string), cast(work_type as string))) as _work_raw
    from source
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
    
    -- Phân loại: chuẩn hóa work_type về tập giá trị cố định (BI + test accepted_values)
    case
        when lower(_work_raw) in ('full-time', 'fulltime', 'full time', 'f/t', 'permanent', 'full_time') then 'Full-time'
        when lower(_work_raw) in ('part-time', 'parttime', 'part time', 'p/t', 'part_time') then 'Part-time'
        when lower(_work_raw) in ('contract', 'temporary', 'temp', 'contractor') then 'Contract'
        when lower(_work_raw) in ('internship', 'intern') then 'Internship'
        when lower(_work_raw) in ('volunteer', 'volunteering') then 'Volunteer'
        when lower(_work_raw) in ('other', 'unknown', 'unspecified') then 'Other'
        when _work_raw is null or _work_raw = '' then 'Other'
        else 'Other'
    end as work_type,
    cast(formatted_work_type as string) as formatted_work_type,
    formatted_experience_level as experience_level,
    
    -- Chỉ số (Metrics)
    applies,
    views,
    
    -- Thời gian (Time)
    cast(posted_date as date) as posted_date,
    listed_time,
    expiry,
    closed_time

from typed
