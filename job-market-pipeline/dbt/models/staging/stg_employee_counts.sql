with source as (
    select * from {{ source('raw', 'companies_employee_counts') }}
)
select
    cast(company_id as string) as company_id,
    employee_count,
    follower_count,
    time_recorded -- Đây có thể chuyển thành Date sau này
from source
