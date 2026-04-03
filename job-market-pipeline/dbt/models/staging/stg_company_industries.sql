with source as (
    select * from {{ source('raw', 'companies_company_industries') }}
)
select
    cast(company_id as string) as company_id,
    industry
from source
