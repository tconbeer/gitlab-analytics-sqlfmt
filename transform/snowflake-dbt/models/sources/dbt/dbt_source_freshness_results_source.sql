{{ config({"materialized": "view"}) }}

with
    source as (select * from {{ source("dbt", "freshness") }}),
    v0parsed as (

        select
            regexp_replace(s.path, '\\[|\\]|''', '')::varchar as source_unique_id,
            replace(
                regexp_replace(s.path, '\\[|\\]|''', ''), 'source.gitlab_snowflake.', ''
            )::varchar as schema_table_name,
            split_part(schema_table_name, '.', 1) as schema_name,
            split_part(schema_table_name, '.', -1) as table_name,
            s.value['max_loaded_at']::timestamp as latest_load_at,
            s.value['max_loaded_at_time_ago_in_s']::float as time_since_loaded_seconds,
            s.value['state']::varchar as source_freshness_state,
            s.value['snapshotted_at']::timestamp as freshness_observed_at,
            {{
                dbt_utils.surrogate_key(
                    ["schema_table_name", "freshness_observed_at"]
                )
            }} as freshness_unique_key,
            'PRE 0.19.0' as dbt_version,
            'https://schemas.getdbt.com/dbt/sources/v0.json' as schema_version,
            uploaded_at
        from source
        inner join lateral flatten(jsontext['sources']) s
        where
            jsontext['metadata']['dbt_version'] is null
            -- impossible to know what freshness is, so filtered out
            and s.value['state']::varchar != 'runtime error'
            and s.value['max_loaded_at']::timestamp is not null  -- latest_load_at
            -- freshness_observed_at
            and s.value['snapshotted_at']::timestamp is not null
            -- time_since_loaded_seconds
            and s.value['max_loaded_at_time_ago_in_s']::float is not null

    ),
    v1parsed as (

        select
            s.value['unique_id']::varchar as source_unique_id,
            replace(s.value['unique_id'], 'source.gitlab_snowflake.', '')::varchar
            as schema_table_name,
            split_part(schema_table_name, '.', 1) as schema_name,
            split_part(schema_table_name, '.', -1) as table_name,
            s.value['max_loaded_at']::timestamp as latest_load_at,
            s.value['max_loaded_at_time_ago_in_s']::float as time_since_loaded_seconds,
            s.value['status']::varchar as source_freshness_state,
            s.value['snapshotted_at']::timestamp as freshness_observed_at,
            {{
                dbt_utils.surrogate_key(
                    ["schema_table_name", "freshness_observed_at"]
                )
            }} as freshness_unique_key,
            jsontext['metadata']['dbt_version']::varchar as dbt_version,
            jsontext['metadata']['dbt_schema_version']::varchar as schema_version,
            uploaded_at
        from source
        inner join lateral flatten(jsontext['results']) s
        where
            jsontext['metadata']['dbt_version'] is not null
            and s.value['max_loaded_at']::timestamp is not null  -- latest_load_at
            -- freshness_observed_at
            and s.value['snapshotted_at']::timestamp is not null
            -- time_since_loaded_seconds
            and s.value['max_loaded_at_time_ago_in_s']::float is not null
    )


select *
from v1parsed

union

select *
from v0parsed
