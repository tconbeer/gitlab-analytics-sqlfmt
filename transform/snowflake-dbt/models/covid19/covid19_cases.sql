with
    source as (select * from {{ source("covid19", "cases") }}),
    renamed as (

        select
            country_region::varchar as country_region,
            province_state::varchar as province_state,
            date::date as date,
            case_type::varchar as case_type,
            cases::number as case_count,
            long::float as longitude,
            lat::float as latitude,
            difference::number as case_count_change,
            last_updated_date::timestamp as last_updated_date
        from source

    )

select *
from renamed
