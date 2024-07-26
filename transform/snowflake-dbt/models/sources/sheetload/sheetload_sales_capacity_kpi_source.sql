with
    source as (select * from {{ source("sheetload", "sales_capacity_kpi") }}),
    renamed as (

        select
            month::date as month,
            ifnull(target, 0) as target,
            ifnull(actual, 0) as actual,
            to_timestamp(to_numeric("_UPDATED_AT"))::timestamp as last_updated_at
        from source

    )

select *
from renamed
