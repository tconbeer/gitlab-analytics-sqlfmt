{{ config({"materialized": "incremental", "unique_key": "id_full_ping_name"}) }}

with
    usage_data as (

        select
            {{
                dbt_utils.star(
                    from=ref("version_usage_data"),
                    except=["LICENSE_STARTS_AT", "LICENSE_EXPIRES_AT"],
                )
            }}
        from {{ ref("version_usage_data") }}

    )

select
    {{ dbt_utils.surrogate_key(["id", "path"]) }} as id_full_ping_name,
    id,
    f.path as ping_name,
    created_at,
    replace(f.path, '.', '_') as full_ping_name,
    f.value as ping_value

from usage_data, lateral flatten(input => usage_data.stats_used, recursive => true) f
where
    is_object(f.value) = false and stats_used is not null and full_ping_name in (
        select full_ping_name from {{ ref("version_usage_stats_list") }}
    )
    {% if is_incremental() %}
    and created_at >= (select max(created_at) from {{ this }})
    {% endif %}
