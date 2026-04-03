with postings as (
    select * from {{ ref('stg_job_postings') }}
),

salaries as (
    -- Group salaries by job_id to avoid duplicates
    select 
        job_id,
        avg(med_salary) as avg_med_salary,
        max(max_salary) as max_salary_cap,
        min(min_salary) as min_salary_floor
    from {{ ref('stg_job_salaries') }}
    group by 1
),

industries as (
    -- Aggregating industries into a clean list
    select 
        ji.job_id,
        string_agg(i.industry_name, ', ') as industry_list
    from {{ ref('stg_job_industries') }} ji
    join {{ ref('stg_industries') }} i on ji.industry_id = i.industry_id
    group by 1
),

skills as (
    -- Aggregating skills into a clean list
    select 
        js.job_id,
        string_agg(s.skill_name, ', ') as skill_list
    from {{ ref('stg_job_skills') }} js
    join {{ ref('stg_skills') }} s on js.skill_abr = s.skill_abr
    group by 1
),

joined as (
    select
        p.*,
        s.avg_med_salary,
        s.max_salary_cap,
        s.min_salary_floor,
        i.industry_list,
        sk.skill_list,
        -- Thêm logic xử lý lương rác (Outlier Handling)
        case 
            when s.avg_med_salary > 1000000 then null -- Ví dụ: Lương > 1 triệu USD/tháng (vô lý)
            when s.avg_med_salary <= 0 then null
            else s.avg_med_salary
        end as salary_cleaned
    from postings p
    left join salaries s on p.job_id = s.job_id
    left join industries i on p.job_id = i.job_id
    left join skills sk on p.job_id = sk.job_id
)

select * from joined
