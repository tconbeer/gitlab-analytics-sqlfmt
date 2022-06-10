{{ config({"materialized": "incremental", "unique_key": "ping_name"}) }}
with version_usage_data as (select * from {{ ref("version_usage_data") }})

select distinct
    stats.path as ping_name, replace (stats.path, '.', '_') as full_ping_name
from version_usage_data
inner join
    lateral flatten(input => version_usage_data.stats_used, recursive => true) as stats
where
    is_object(stats.value) = false
    {% if is_incremental() %}
    -- This prevents new metrics, and therefore columns, from being added to
    -- downstream tables.
    and full_ping_name in (select full_ping_name from {{ this }})
    {% endif %}
