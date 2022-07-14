with
    source as (

        select
            *,
            rank() OVER (
                partition by date_trunc('day', uploaded_at) order by uploaded_at desc
            ) as rank
        from {{ source("gitlab_data_yaml", "categories") }}
        order by uploaded_at desc

    ),
    split_acquisition_info as (

        select
            category.value['name']::varchar as category_name,
            category.value['stage']::varchar as category_stage,
            acquisition.value::variant as acquisition_object,
            acquisition.key::varchar as acquisition_key,
            acquisition_info.value as info_object,
            acquisition_info.key as info_key,
            date_trunc('day', uploaded_at)::date as snapshot_date,
            rank
        from
            source,
            lateral flatten(input => parse_json(jsontext), outer => true) category,
            lateral flatten(
                input => parse_json(category.value), outer => true
            ) acquisition,
            table(
                flatten(input => acquisition.value, recursive => true)
            ) acquisition_info
        where
            acquisition.key ilike 'acquisition_%'
            and acquisition_info.key in ('name', 'start_date', 'end_date')

    ),
    info_combined as (

        -- Combine back the list of objects about each acquisition into a single object
        select
            category_name,
            category_stage,
            rank,
            snapshot_date,
            acquisition_key,
            object_agg(info_key, info_object) acquisition_info
        from split_acquisition_info {{ dbt_utils.group_by(n=5) }}

    ),
    info_parsed as (

        -- Parse the object about each acquisition
        select
            category_name,
            category_stage,
            rank,
            snapshot_date,
            acquisition_key,
            acquisition_info['name']::varchar as acquisition_name,
            acquisition_info['start_date']::date as acquisition_start_date,
            acquisition_info['end_date']::date as acquisition_end_date
        from info_combined

    )

select *
from info_parsed
