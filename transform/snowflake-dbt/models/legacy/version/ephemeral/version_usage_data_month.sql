{{ config({"materialized": "table"}) }}

{% set version_usage_stats_list = dbt_utils.get_column_values(
    table=ref("version_usage_stats_list"),
    column="full_ping_name",
    max_records=1000,
    default=[],
) %}


with
    usage_data as (select * from {{ ref("version_usage_data_unpacked") }}),
    usage_data_month_base as (

        select
            md5(
                usage_data.uuid || date_trunc('month', usage_data.created_at)::date
            ) as unique_key,
            md5(
                usage_data.uuid
                || (
                    date_trunc('month', usage_data.created_at) + interval '1 month'
                )::date
            ) as next_unique_key,
            uuid,
            ping_source,
            date_trunc('month', created_at)::date as created_at,
            max(id) as ping_id,
            max(company) as company,
            max(instance_user_count) as instance_user_count,
            max(edition) as edition,
            max(main_edition) as main_edition,
            max(edition_type) as edition_type,
            max(git_version) as git_version,
            max(gitaly_version) as gitaly_version,
            max(gitaly_servers) as gitaly_servers,
            max(ldap_enabled) as ldap_enabled,
            max(gitpod_enabled) as gitpod_enabled,

            {% for ping_name in version_usage_stats_list %}
            max({{ ping_name }}) as {{ ping_name }}
            {%- if not loop.last %}, {% endif -%}
            {%- endfor %}

        from usage_data {{ dbt_utils.group_by(n=5) }}
    )

select
    this_month.*,
    case
        when next_month.next_unique_key is not null then false else true
    end as churned_next_month
from usage_data_month_base this_month
left join
    usage_data_month_base as next_month
    on this_month.next_unique_key = next_month.unique_key
