with
    source as (select * from {{ source("sheetload", "maxmind_ranges") }}),
    parsed as (

        select
            network_start_ip::varchar as ip_range_first_ip,
            network_last_ip::varchar as ip_range_last_ip,
            parse_ip(ip_range_first_ip, 'inet')['ip_fields'][0]::number
            as ip_range_first_ip_numeric,
            parse_ip(ip_range_last_ip, 'inet')['ip_fields'][0]::number
            as ip_range_last_ip_numeric,
            geoname_id::number as geoname_id,
            registered_country_geoname_id::number as registered_country_geoname_id,
            represented_country_geoname_id::number as represented_country_geoname_id,
            is_anonymous_proxy::boolean as is_anonymous_proxy,
            is_satellite_provider::boolean as is_satellite_provider
        from source

    )

select *
from parsed
