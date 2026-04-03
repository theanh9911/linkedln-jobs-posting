with companies as (
    select * from {{ ref('stg_companies') }}
),

company_industries as (
    select 
        company_id,
        string_agg(industry, ', ') as industry_list
    from {{ ref('stg_company_industries') }}
    group by 1
),

company_specialities as (
    select 
        company_id,
        string_agg(speciality, ', ') as speciality_list
    from {{ ref('stg_company_specialities') }}
    group by 1
),

final as (
    select
        c.*,
        i.industry_list as company_industry_list,
        s.speciality_list as company_speciality_list
    from companies c
    left join company_industries i on c.company_id = i.company_id
    left join company_specialities s on c.company_id = s.company_id
)

select * from final
