with
    source as (

        select
            jsontext,
            date_trunc(day, uploaded_at) as uploaded_at,
            rank() OVER (
                partition by date_trunc('day', uploaded_at) order by uploaded_at desc
            ) as rank
        from {{ source("gitlab_data_yaml", "geo_zones") }}
        order by uploaded_at desc

    ),
    intermediate as (

        select
            geozones.value['title']::varchar as geozone_title,
            geozones.value['factor']::varchar as geozone_factor,
            geozones.index as geozone_index,
            additional_fields.key::varchar as info_key,
            field_info.value::varchar as field_value,
            iff(uploaded_at = '2021-01-04', '2021-01-01', uploaded_at) as uploaded_at
        from
            source,
            lateral flatten(input => parse_json(jsontext), outer => true) geozones,
            lateral flatten(
                input => parse_json(geozones.value), outer => true
            ) additional_fields,
            table(
                flatten(input => additional_fields.value, recursive => true)) field_info

    ),
    unioned as (

        select distinct
            geozone_title,
            geozone_factor,
            'United States' as country,
            field_value as state_or_province,
            uploaded_at
        from intermediate
        where info_key in ('states_or_provinces')

        union all

        select distinct
            geozone_title,
            geozone_factor,
            field_value as country,
            null as state_or_province,
            uploaded_at
        from intermediate
        where info_key in ('countries')

    )

select
    {{
        dbt_utils.surrogate_key(
            [
                "geozone_title",
                "geozone_factor",
                "country",
                "state_or_province",
                "geozone_factor",
            ]
        )
    }} as unique_key, unioned.*
from unioned
