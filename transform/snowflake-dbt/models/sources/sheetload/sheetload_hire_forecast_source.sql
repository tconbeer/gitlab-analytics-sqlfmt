with
    source as (select * from {{ source("sheetload", "hire_forecast") }}),
    renamed as (

        select
            function::varchar as function,
            department::varchar as department,
            month_year::date as month_year,
            try_to_number(forecast) as forecast
        from source

    )

select *
from renamed
where forecast is not null
