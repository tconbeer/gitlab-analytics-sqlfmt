with
    source as (select * from {{ source("google_analytics_360", "ga_session") }}),
    renamed as (

        select
            -- Keys
            visit_id::float as visit_id,
            visitor_id::varchar as visitor_id,
            visit_start_time::timestamp_tz as visit_start_time,

            -- Info
            date::date as session_date,
            client_id::varchar as client_id,
            visit_number::float as visit_number,
            total_visits::float as total_visits,
            total_pageviews::float as total_pageviews,
            total_screenviews::float as total_screenviews,
            total_unique_screenviews::float as total_unique_screenviews,
            total_hits::float as total_hits,
            total_new_visits::float as total_new_visits,
            total_time_on_screen::float as total_time_on_screen,
            total_time_on_site::float as total_time_on_site,
            traffic_source_source::varchar as traffic_source,
            traffic_source_referral_path::varchar as traffic_source_referral_path,
            traffic_source_campaign::varchar as traffic_source_campaign,
            traffic_source_medium::varchar as traffic_source_medium,
            traffic_source_keyword::varchar as traffic_source_keyword,
            device_device_category::varchar as device_category,
            device_browser::varchar as device_browser,
            device_browser_version::varchar as device_browser_version,
            device_browser_size::varchar as device_browser_size,
            device_operating_system::varchar as device_operating_system,
            device_operating_system_version::varchar as device_operating_system_version,
            geo_network_continent::varchar as geo_network_continent,
            geo_network_sub_continent::varchar as geo_network_sub_continent,
            geo_network_country::varchar as geo_network_country,
            geo_network_city::varchar as geo_network_city

        from source

    )

select *
from renamed
