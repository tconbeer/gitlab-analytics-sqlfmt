
{{ config({"materialized": "incremental", "unique_key": "instance_path_id"}) }}

with
    data as (

        select *
        from {{ ref("version_usage_data") }}
        {% if is_incremental() %}

        where created_at >= (select max(created_at) from {{ this }}) {% endif %}

    ),
    flattened as (

        select
            {{ dbt_utils.surrogate_key(["id", "path"]) }} as instance_path_id,
            uuid as instance_id,
            id as ping_id,
            edition,
            host_id,
            created_at,
            version,
            major_minor_version,
            major_version,
            minor_version,
            path as metrics_path,
            value as metric_value
        from data, lateral flatten(input => raw_usage_data_payload, recursive => true)

    )

select *
from flattened
