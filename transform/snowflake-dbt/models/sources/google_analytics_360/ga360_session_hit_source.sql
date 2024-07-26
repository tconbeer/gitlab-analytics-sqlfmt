with
    source as (select * from {{ source("google_analytics_360", "session_hit") }}),
    renamed as (

        select
            -- Keys
            visit_id::float as visit_id,
            visitor_id::varchar as visitor_id,

            -- Info
            visit_start_time::timestamp as visit_start_at,
            hit_number::number as hit_number,
            dateadd('millisecond', time, visit_start_at) as hit_at,
            is_entrance::boolean as is_entrance,
            is_exit::boolean as is_exit,
            referer::varchar as referer,
            type::varchar as hit_type,
            data_source::varchar as data_source,
            page_hostname::varchar as host_name,
            page_page_path::varchar as page_path,
            page_page_title::varchar as page_title,
            event_info_category::varchar as event_category,
            event_info_action::varchar as event_action,
            event_info_label::varchar as event_label

        from source

    )

select *
from renamed
