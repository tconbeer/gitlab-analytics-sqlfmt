with
    source as (
        /* 
       Selecting only non error results as there will be nothing to 
       flatten  if there was an error
       */
        select
            jsontext,
            uploaded_at,
            lead(uploaded_at, 1) OVER (order by uploaded_at) as lead_uploaded_at,
            max(uploaded_at) OVER () as max_uploaded_at
        from {{ source("gitlab_data_yaml", "location_factors") }}
        where not contains(jsontext, 'Server Error')

    ),
    grouped as (
        /*
      Reducing to only the changed values to reduce processing load
      and create a contiguious range of data.
      */
        select distinct
            jsontext,
            min(uploaded_at) OVER (partition by jsontext) as valid_from,
            max(lead_uploaded_at) OVER (partition by jsontext) as valid_to,
            iff(valid_to = max_uploaded_at, true, false) as is_current
        from source

    ),
    level_1 as (

        select
            valid_from,
            valid_to,
            is_current,
            level_1.value['area']::varchar as area_level_1,
            level_1.value['country']::varchar as country_level_1,
            level_1.value['locationFactor']::number(6, 3) as locationfactor_level_1,
            level_1.value['factor']::number(6, 3) as factor_level_1,
            level_1.value['metro_areas']::variant as metro_areas_level_1,
            level_1.value['states_or_provinces']::variant as states_or_provinces_level_1
        from grouped
        inner join
            lateral flatten(input => try_parse_json(jsontext), outer => true) as level_1

    ),
    level_1_metro_areas as (

        select
            level_1.*,
            level_1_metro_areas.value['name']::varchar as metro_areas_name_level_2,
            level_1_metro_areas.value['factor']::number(
                6, 3
            ) as metro_areas_factor_level_2,
            level_1_metro_areas.value['sub_location']::varchar
            as metro_areas_sub_location_level_2
        from level_1
        inner join
            lateral flatten(
                input => try_parse_json(metro_areas_level_1), outer => true
            ) as level_1_metro_areas
        union all
        -- For the country level override when there is also a metro area
        select
            level_1.*,
            null as metro_areas_name_level_2,
            null as metro_areas_factor_level_2,
            null as metro_areas_sub_location_level_2
        from level_1
        where factor_level_1 is not null and metro_areas_level_1 is not null

    ),
    level_1_states_or_provinces as (

        select
            level_1_metro_areas.*,
            level_1_states_or_provinces.value['name']::varchar
            as states_or_provinces_name_level_2,
            level_1_states_or_provinces.value['factor']::number(
                6, 3
            ) as states_or_provinces_factor_level_2,
            level_1_states_or_provinces.value['metro_areas']::variant
            as states_or_provinces_metro_areas_level_2
        from level_1_metro_areas
        inner join
            lateral flatten(
                input => try_parse_json(states_or_provinces_level_1),
                outer => true
            ) as level_1_states_or_provinces

    ),
    level_2_states_or_provinces_metro_areas as (

        select
            level_1_states_or_provinces.*,
            level_2_states_or_provinces_metro_areas.value['name']::varchar
            as states_or_provinces_metro_areas_name_level_2,
            level_2_states_or_provinces_metro_areas.value['factor']::number(
                6, 3
            ) as states_or_provinces_metro_areas_factor_level_2
        from level_1_states_or_provinces
        inner join
            lateral flatten(
                input => try_parse_json(states_or_provinces_metro_areas_level_2),
                outer => true
            ) as level_2_states_or_provinces_metro_areas

    )

select
    valid_from,
    valid_to,
    is_current,
    area_level_1,
    country_level_1,
    locationfactor_level_1,
    factor_level_1,
    metro_areas_name_level_2,
    metro_areas_factor_level_2,
    metro_areas_sub_location_level_2,
    states_or_provinces_name_level_2,
    states_or_provinces_factor_level_2,
    states_or_provinces_metro_areas_name_level_2,
    states_or_provinces_metro_areas_factor_level_2
from level_2_states_or_provinces_metro_areas
