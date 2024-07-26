with source as (select * from {{ ref("sheetload_hp_dvr_source") }})

select
    date::varchar as date,
    region::varchar as region,
    country::varchar as country,
    name::varchar as name,
    numberrange::number as numberrange,
    alphanumeric::varchar as alphanumeric,
    _updated_at::number as _updated_at
from source
