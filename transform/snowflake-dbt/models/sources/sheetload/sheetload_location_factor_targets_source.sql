with
    source as (select * from {{ source("sheetload", "location_factor_targets") }}),
    renamed as (

        select
            department::varchar as department, target::float as location_factor_target
        from source

    )

select *
from renamed
