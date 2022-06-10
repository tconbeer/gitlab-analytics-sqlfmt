with
    source as (select * from {{ source("sheetload", "days_to_close") }}),
    renamed as (

        select
            close_month::date as close_month,
            days_to_close::number as days_to_close,
            days_to_close_target::number as days_to_close_target
        from source

    )

select *
from renamed
