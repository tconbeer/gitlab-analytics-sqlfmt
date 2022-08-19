with
    source as (

        select
            *,
            rank() over (
                partition by date_trunc('day', uploaded_at) order by uploaded_at desc
            ) as rank
        from {{ source("gitlab_data_yaml", "feature_flags") }}
        order by uploaded_at desc

    ),
    intermediate as (

        select
            d.value as data_by_row,
            date_trunc('day', uploaded_at)::date as snapshot_date,
            rank
        from source, lateral flatten(input => parse_json(jsontext), outer => true) d

    ),
    renamed as (

        select
            data_by_row['name']::varchar as name,
            data_by_row['type']::varchar as type,
            data_by_row['milestone']::varchar as milestone,
            data_by_row['default_enabled']::varchar as is_default_enabled,
            data_by_row['group']::varchar as gitlab_group,
            data_by_row['introduced_by_url']::varchar
            as introduced_by_merge_request_url,
            data_by_row['rollout_issue_url']::varchar as rollout_issue_url,
            snapshot_date,
            rank
        from intermediate

    ),
    casting as (

        select
            name,
            type,
            milestone,
            try_to_boolean(is_default_enabled) as is_default_enabled,
            gitlab_group,
            introduced_by_merge_request_url,
            rollout_issue_url,
            snapshot_date,
            rank
        from renamed
    )

select *
from casting
