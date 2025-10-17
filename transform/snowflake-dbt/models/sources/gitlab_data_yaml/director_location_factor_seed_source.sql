with
    source as (select * from {{ ref("director_location_factors") }}),
    formated as (

        select
            country::varchar as country,
            locality::varchar as locality,
            factor::number(6, 3) as factor,
            valid_from::date as valid_from,
            coalesce(valid_to, {{ var("tomorrow") }})::date as valid_to,
            is_current::boolean as is_current
        from source

    )

select *
from formated
