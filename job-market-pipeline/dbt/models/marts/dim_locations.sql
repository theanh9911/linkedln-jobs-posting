with locations as (
    -- Tách chuỗi địa lý từ postings hoặc dùng cột location có sẵn
    select distinct 
        location as location_name,
        case 
            when location like '%Remote%' then true 
            else false 
        end as is_remote_friendly
    from {{ ref('stg_job_postings') }}
)
select
    -- Tạo một ID giả lập từ tên địa điểm nếu không có ID gốc
    row_number() over (order by location_name) as location_id,
    location_name,
    is_remote_friendly
from locations
