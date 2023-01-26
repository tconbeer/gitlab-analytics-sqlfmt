{{ config({"materialized": "view"}) }}

with
    source as (select * from {{ source("dbt", "run") }}),
    flattened as (

        select
            d.value as data_by_row,
            jsontext['metadata']['dbt_version']::varchar as dbt_version,
            jsontext['metadata']['dbt_schema_version']::varchar as schema_version,
            coalesce(
                jsontext['metadata']['generated_at'], jsontext['generated_at']
            )::timestamp_ntz as generated_at,
            uploaded_at
        from source
        inner join
            lateral flatten(input => parse_json(jsontext['results']), outer => true) d

    ),
    v1model_parsed_out as (

        select
            data_by_row['execution_time']::float as model_execution_time,
            data_by_row['unique_id']::varchar as model_unique_id,
            ifnull(data_by_row['status']::varchar, false) as status,
            ifnull(data_by_row['message']::varchar, false) as message,
            timing.value['started_at']::timestamp as compilation_started_at,
            timing.value['completed_at']::timestamp as compilation_completed_at,
            uploaded_at,  -- uploaded_at
            dbt_version,
            schema_version,
            generated_at,
            {{
                dbt_utils.surrogate_key(
                    ["model_unique_id", "compilation_started_at", "uploaded_at"]
                )
            }} as run_unique_key
        from flattened
        left join
            lateral flatten(input => data_by_row['timing']::array, outer => true) timing
            on ifnull(timing.value['name'], 'compile') = 'compile'
        where dbt_version is not null

    ),
    v0model_parsed_out as (

        select
            data_by_row['execution_time']::float as model_execution_time,
            data_by_row['node']['unique_id']::varchar as model_unique_id,
            case
                when data_by_row['skip']::boolean = true
                then 'skipped'
                when data_by_row['error']::varchar is not null
                then 'error'
                else 'success'
            end as status,
            ifnull(data_by_row['error']::varchar, 'SUCCESS 1') as message,
            timing.value['started_at']::timestamp as compilation_started_at,
            timing.value['completed_at']::timestamp as compilation_completed_at,
            uploaded_at,  -- uploaded_at
            'PRE 0.19.0' as dbt_version,
            'https://schemas.getdbt.com/dbt/run-results/v0.json' as schema_version,
            generated_at,
            {{
                dbt_utils.surrogate_key(
                    ["model_unique_id", "compilation_started_at", "uploaded_at"]
                )
            }} as run_unique_key
        from flattened
        left join
            lateral flatten(input => data_by_row['timing']::array, outer => true) timing
            on ifnull(timing.value['name'], 'compile') = 'compile'
        where dbt_version is null
    )

select *
from v0model_parsed_out

union

select *
from v1model_parsed_out
