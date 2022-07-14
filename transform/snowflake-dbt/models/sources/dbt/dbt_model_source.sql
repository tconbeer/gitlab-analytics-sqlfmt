{{ config({"unique_key": "run_unique_key"}) }}

with
    source as (

        select *
        from {{ source("dbt", "manifest") }}
        {% if is_incremental() %}
        where uploaded_at > (select max(uploaded_at) from {{ this }}) {% endif %}

    ),
    nodes as (

        select
            d.value as data_by_row,
            jsontext['metadata'] ['dbt_version']::varchar as dbt_version,
            jsontext['metadata'] ['dbt_schema_version']::varchar as schema_version,
            jsontext['metadata'] ['generated_at']::timestamp as generated_at,
            uploaded_at
        from source
        inner join
            lateral flatten(input => parse_json(jsontext['nodes']), outer => true) d

    ),
    parsed as (

        select
            data_by_row['unique_id']::varchar as unique_id,
            data_by_row['name']::varchar as name,
            data_by_row['alias']::varchar as alias,
            data_by_row['database']::varchar as database_name,
            data_by_row['schema']::varchar as schema_name,
            data_by_row['package_name']::varchar as package_name,
            data_by_row['tags']::array as tags,
            data_by_row['refs']::array as references,
            {{ dbt_utils.surrogate_key(["unique_id", "generated_at"]) }}
            as run_unique_key,
            dbt_version,
            schema_version,
            generated_at,
            uploaded_at
        from nodes
        where data_by_row['resource_type']::varchar = 'model'

    )

select *
from parsed
