with
    source as (

        select
            *,
            date_trunc('day', uploaded_at)::date as snapshot_date,
            rank() OVER (
                partition by date_trunc('day', uploaded_at) order by uploaded_at desc
            ) as rank
        from {{ source("gitlab_data_yaml", "location_factors") }}
        order by uploaded_at desc

    ),
    original_yaml_file_format as (

        select
            d.value['area']::varchar as area,
            d.value['country']::varchar as country,
            d.value['locationFactor']::float as location_factor,
            snapshot_date,
            rank
        from source, lateral flatten(input => parse_json(jsontext), outer => true) d
        where snapshot_date <= '2020-10-21'

    ),
    united_states as (

        select distinct
            initial_unnest.value['country']::varchar as country,
            countries_info.value['name']::varchar as location_state,
            metro_areas.value['name']::varchar as metro_area,
            metro_areas.value['factor']::varchar as location_factor,
            snapshot_date,
            rank
        from
            source,
            lateral flatten(
                input => parse_json(jsontext), outer => true
            ) initial_unnest,
            lateral flatten(
                input => parse_json(initial_unnest.value), outer => true
            ) countries,
            table(flatten(input => countries.value, recursive => true)) countries_info,
            table(flatten(input => countries_info.value, recursive => true)) metro_areas
        where
            snapshot_date >= '2021-01-04'
            and countries_info.index is not null
            and metro_areas.index is not null
            and country = 'United States'

    ),
    other as (

        select distinct
            initial_unnest.value['country']::varchar as country,
            countries_info.value['sub_location']::varchar as state,
            countries_info.value['name']::varchar as metro_area,
            countries_info.value['factor']::varchar as location_factor,
            snapshot_date,
            rank,
            countries_info.index as country_info_index
        from
            source,
            lateral flatten(
                input => parse_json(jsontext), outer => true
            ) initial_unnest,
            lateral flatten(
                input => parse_json(initial_unnest.value), outer => true
            ) countries,
            table(flatten(input => countries.value, recursive => true)) countries_info
        where snapshot_date >= '2021-01-04' and countries_info.index is not null

    ),
    countries_without_metro_areas as (

        select distinct
            case
                when initial_unnest.value['country']::varchar = 'Israel'
                then 'All'
                when initial_unnest.value['metro_areas']::varchar is not null
                then 'Everywhere else'
                else 'All'
            end as area,
            initial_unnest.value['country']::varchar as country,
            initial_unnest.value['factor']::varchar as location_factor,
            snapshot_date,
            rank
        from
            source,
            lateral flatten(
                input => parse_json(jsontext), outer => true
            ) initial_unnest,
            lateral flatten(
                input => parse_json(initial_unnest.value), outer => true
            ) countries
        where snapshot_date >= '2021-01-04' and location_factor is not null

    ),
    unioned as (

        select area, country, location_factor, snapshot_date, rank
        from original_yaml_file_format

        union all

        select
            metro_area || ', ' || location_state as area,
            country,
            location_factor,
            snapshot_date,
            rank
        from united_states

        union all

        select
            metro_area || iff(state is null, '', ', ' || state) as metro_area,
            country,
            location_factor,
            snapshot_date,
            rank
        from other
        where country != 'United States'

        union  all

        select area as metro_area, country, location_factor, snapshot_date, rank
        from countries_without_metro_areas

        union all

        select metro_area, country, location_factor, snapshot_date, rank
        from other
        where
            country = 'United States'
            and location_factor is not null
            and metro_area in ('Hawaii', 'Washington DC')
    -- -this is to capture data points with just metro area  
    )

select *
from unioned
where rank = 1
