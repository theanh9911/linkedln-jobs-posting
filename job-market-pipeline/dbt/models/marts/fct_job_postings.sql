{{
    config(
        materialized='incremental',
        unique_key='job_id',
        partition_by={
            "field": "posted_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=["company_id", "experience_level"]
    )
}}

with processed_postings as (
    select * from {{ ref('int_job_postings_joined') }}
    
    {% if is_incremental() %}
        -- Chỉ lấy các bản ghi có ngày đăng tin lớn hơn ngày lớn nhất hiện có trong bảng
        where posted_date > (select max(posted_date) from {{ this }})
    {% endif %}
),

valid_company as (
    select company_id from {{ ref('dim_companies') }}
),

with_orphan_company_fixed as (
    select
        pp.* except (company_id),
        case
            when vc.company_id is not null then pp.company_id
            else null
        end as company_id
    from processed_postings pp
    left join valid_company vc on pp.company_id = vc.company_id
)

select
    -- Khóa (Keys)
    job_id,
    company_id,
    
    -- Thông tin cơ bản
    title,
    location,
    experience_level,
    work_type,
    
    -- Lương đã qua xử lý (Metrics)
    avg_med_salary,
    max_salary_cap,
    min_salary_floor,
    salary_cleaned as salary_clean,
    
    -- Danh sách (Lists để BI query nhanh)
    industry_list,
    skill_list,
    
    -- Chỉ số hiệu suất (Metrics)
    applies,
    views,
    (applies / nullif(views, 0)) * 100 as apply_rate_pct,
    
    -- Thời gian (Partition key)
    posted_date,
    listed_time

from with_orphan_company_fixed
