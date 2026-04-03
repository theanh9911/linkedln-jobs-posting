{{
    config(
        materialized='table',
        unique_key='company_id'
    )
}}

with companies_refined as (
    select * from {{ ref('int_companies_refined') }}
),

employees as (
    -- Lấy bản ghi mới nhất về số người theo dõi cho mỗi công ty
    select 
        company_id,
        employee_count,
        follower_count,
        row_number() over (partition by company_id order by time_recorded desc) as rn
    from {{ ref('stg_employee_counts') }}
),

final as (
    select
        c.*,
        e.employee_count as last_known_employee_count,
        e.follower_count as last_known_follower_count
    from companies_refined c
    left join employees e on c.company_id = e.company_id and e.rn = 1
)

select * from final
