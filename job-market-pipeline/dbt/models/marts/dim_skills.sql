with skills as (
    select * from {{ ref('stg_skills') }}
)
select
    skill_abr as skill_id,
    skill_name,
    case 
        when skill_name in ('Python', 'SQL', 'Java', 'C++', 'Go') then 'Hard Skill / Coding'
        when skill_name in ('Management', 'Leadership', 'Communication') then 'Soft Skill'
        else 'Other'
    end as skill_category
from skills
