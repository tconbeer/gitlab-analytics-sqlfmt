{% set version_usage_stats_list = dbt_utils.get_column_values(
    table=ref("version_usage_stats_list"),
    column="full_ping_name",
    max_records=1000,
    default=[],
) %}

with
    mom_change as (

        select
            md5(uuid || created_at) as unique_key,
            uuid,
            created_at,
            ping_source,
            company,
            edition,
            main_edition,
            edition_type,
            ldap_enabled,
            gitpod_enabled,
            {% for ping_name in version_usage_stats_list %}
            {{ ping_name }},
            {{ monthly_change(ping_name) }},
            {{ case_when_boolean_int(ping_name) }} as {{ ping_name }}_active

            {{ "," if not loop.last }}
            {% endfor %}

        from {{ ref("version_usage_data_month") }}
        where
            uuid
            not in (select uuid from {{ ref("version_blacklisted_instance_uuid") }})

    )

select *
from mom_change
