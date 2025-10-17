with
    source as (

        select *
        from {{ source("sheetload", "location_factor_temporary_2020_december") }}

    ),
    renamed as (

        select
            employee_number::varchar as employee_number,
            location_factor::float as location_factor
        from source

    )

select *
from renamed
