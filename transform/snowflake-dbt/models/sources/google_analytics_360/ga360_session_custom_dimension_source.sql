with
    source as (

        select *
        from {{ source("google_analytics_360", "ga_session_custom_dimension") }}

    ),
    renamed as (

        select
            -- Keys
            index::float as dimension_index,
            visit_id::float as visit_id,
            visitor_id::varchar as visitor_id,

            -- Info
            value::varchar as dimension_value,
            visit_start_time::timestamp_tz as visit_start_time

        from source

    )

select *
from renamed
