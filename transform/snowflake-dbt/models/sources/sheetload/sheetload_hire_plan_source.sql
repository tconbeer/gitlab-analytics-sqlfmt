with
    source as (select * from {{ source("sheetload", "hire_plan") }}),
    renamed as (

        select
            function::varchar as function,
            department::varchar as department,
            month_year::date as month_year,
            plan::number as plan
        from source

    )

select *
from renamed
