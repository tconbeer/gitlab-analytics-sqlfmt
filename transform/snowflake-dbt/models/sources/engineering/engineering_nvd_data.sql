with
    source as (select * from {{ source("engineering", "nvd_data") }}),
    renamed as (select "0"::number as year, "1"::number as count from source)

select *
from renamed
