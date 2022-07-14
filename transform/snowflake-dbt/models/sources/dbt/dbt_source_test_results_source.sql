{{ config({"unique_key": "test_unique_key"}) }}

with
    source as (

        select *
        from {{ source("dbt", "source_tests") }}
        {% if is_incremental() %}
        where uploaded_at >= (select max(uploaded_at) from {{ this }}) {% endif %}

    ),
    flattened as (

        select
            d.value as data_by_row,
            jsontext['metadata'] ['dbt_version']::varchar as dbt_version,
            jsontext['metadata'] ['dbt_schema_version']::varchar as schema_version,
            coalesce(
                jsontext['metadata'] ['generated_at'], jsontext['generated_at']
            )::timestamp_ntz as generated_at,
            uploaded_at
        from source
        inner join
            lateral flatten(input => parse_json(jsontext['results']), outer => true) d

    ),
    v1model_parsed_out as (

        select
            data_by_row['execution_time']::float as test_execution_time_elapsed,
            data_by_row['unique_id']::varchar as test_unique_id,
            data_by_row['status']::varchar as status,
            data_by_row['message']::varchar as message,
            dbt_version,
            schema_version,
            generated_at,
            {{ dbt_utils.surrogate_key(["test_unique_id", "generated_at"]) }}
            as test_unique_key,
            uploaded_at
        from flattened
        where dbt_version is not null

    ),
    v0model_parsed_out as (

        select
            data_by_row['execution_time']::float as test_execution_time_elapsed,
            data_by_row['node'] ['unique_id']::varchar as test_unique_id,
            case
                when data_by_row['fail'] = 'true'
                then 'fail'
                when data_by_row['warn'] = 'true'
                then 'warn'
                when data_by_row['error']::varchar is not null
                then 'error'
                else 'pass'
            end as status,
            data_by_row['error']::varchar as message,
            'PRE 0.19.0' as dbt_version,
            'https://schemas.getdbt.com/dbt/run-results/v0.json' as schema_version,
            generated_at,
            {{ dbt_utils.surrogate_key(["test_unique_id", "generated_at"]) }}
            as test_unique_key,
            uploaded_at
        from flattened
        where dbt_version is null

    )

select *
from v0model_parsed_out

union

select *
from v1model_parsed_out
