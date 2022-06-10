with
    source as (select * from {{ source("sheetload", "maxmind_countries") }}),
    parsed as (

        select
            geoname_id::number as geoname_id,
            locale_code::varchar as locale_code,
            continent_code::varchar as continent_code,
            continent_name::varchar as continent_name,
            country_iso_code::varchar as country_iso_code,
            country_name::varchar as country_name,
            is_in_european_union::boolean as is_in_european_union
        from source

    )

select *
from parsed
