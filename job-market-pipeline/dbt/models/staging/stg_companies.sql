with source as (
    select * from {{ source('raw', 'companies_companies') }}
)

select
    -- Khóa (Keys)
    cast(company_id as string) as company_id,
    
    -- Thông tin cơ bản (Core info)
    name as company_name,
    description,
    url as company_url,
    
    -- Quy mô & Địa lý (Scale & Geo)
    company_size,
    city,
    state,
    country,
    zip_code,
    address

from source
