with
    source as (

        select
            jsontext,
            uploaded_at,
            lead(uploaded_at, 1) over (order by uploaded_at) as lead_uploaded_at,
            max(uploaded_at) over () as max_uploaded_at
        from {{ source("gitlab_data_yaml", "geo_zones") }}

    ),
    grouped as (
        /*
        Reducing to only the changed values to reduce processing load
        and create a contiguious range of data.
        */
        select distinct
            jsontext,
            min(uploaded_at) over (partition by jsontext) as valid_from,
            max(lead_uploaded_at) over (partition by jsontext) as valid_to,
            iff(valid_to = max_uploaded_at, true, false) as is_current
        from source

    ),
    geozones as (

        select
            valid_from,
            valid_to,
            is_current,
            geozones.value['title']::varchar as geozone_title,
            geozones.value['factor']::number(6, 3) as geozone_factor,
            geozones.value['countries']::variant as geozone_countries,
            geozones.value['states_or_provinces']::variant
            as geozone_states_or_provinces
        from grouped
        inner join
            lateral flatten(input => parse_json(jsontext), outer => true) as geozones

    ),
    countries as (

        select *, countries.value::varchar as country
        from geozones
        inner join
            lateral flatten(
                input => parse_json(geozone_countries), outer => true
            ) as countries

    ),
    states_or_provinces as (

        select *, states_or_provinces.value::varchar as state_or_province
        from countries
        inner join
            lateral flatten(
                input => parse_json(geozone_states_or_provinces), outer => true
            ) as states_or_provinces

    )

select
    valid_from,
    valid_to,
    is_current,
    geozone_title,
    geozone_factor,
    country,
    state_or_province
from states_or_provinces
