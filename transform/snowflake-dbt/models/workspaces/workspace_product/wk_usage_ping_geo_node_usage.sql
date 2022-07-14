{{ config({"materialized": "incremental", "unique_key": "instance_path_id"}) }}

with
    prep_usage_data_flattened as (

        select *
        from {{ ref("poc_prep_usage_data_flattened") }}
        {% if is_incremental() %}

        where created_at >= (select max(created_at) from {{ this }}) {% endif %}

    ),
    data as (

        select *
        from prep_usage_data_flattened
        where
            metrics_path = 'usage_activity_by_stage_monthly.enablement.geo_node_usage'
            and metric_value <> '[]'

    ),
    flattened as (

        select * from data, lateral flatten(input => metric_value, recursive => true)

    )

select
    {{ dbt_utils.surrogate_key(["ping_id", "path"]) }} as instance_path_id,
    instance_id,
    ping_id,
    edition,
    host_id,
    created_at,
    version,
    major_minor_version,
    major_version,
    minor_version,
    regexp_replace(split_part(path, '.', 1), '(\\[|\\])', '') as node_id,
    value as metric_value,
    split_part(path, '.', -1) as metrics_path
from flattened
where index is null
