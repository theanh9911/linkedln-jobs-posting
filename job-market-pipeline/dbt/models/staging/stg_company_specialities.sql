with source as (
    select * from {{ source('raw', 'companies_company_specialities') }}
)
select
    cast(company_id as string) as company_id,
    speciality
from source
