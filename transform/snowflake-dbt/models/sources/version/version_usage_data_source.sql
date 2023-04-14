{{ config({"materialized": "incremental", "unique_key": "id"}) }}

{%- set columns = adapter.get_columns_in_relation(source("version", "usage_data")) -%}

with
    source as (

        select *
        from {{ source("version", "usage_data") }}
        {% if is_incremental() %}
            where created_at >= (select max(created_at) from {{ this }})
        {% endif %}
        qualify row_number() over (partition by id order by updated_at desc) = 1

    ),
    raw_usage_data_payload as (

        select
            *,
            object_construct(
                {% for column in columns %}
                    '{{ column.name | lower }}',
                    coalesce(
                        try_parse_json({{ column.name | lower }}),
                        {{ column.name | lower }}::variant
                    )
                    {% if not loop.last %}, {% endif %}
                {% endfor %}
            ) as raw_usage_data_payload_reconstructed
        from source

    ),
    renamed as (

        select
            id::number as id,
            source_ip::varchar as source_ip,
            version::varchar as version,
            active_user_count::number as instance_user_count,  -- See issue #4872.
            license_md5::varchar as license_md5,
            historical_max_users::number as historical_max_users,
            -- licensee // removed for PII
            license_user_count::number as license_user_count,
            try_cast(license_starts_at as timestamp) as license_starts_at,
            case
                when license_expires_at is null
                then null::timestamp
                when split_part(license_expires_at, '-', 1)::number > 9999
                then '9999-12-30 00:00:00.000 +00'::timestamp
                else license_expires_at::timestamp
            end as license_expires_at,
            parse_json(license_add_ons) as license_add_ons,
            recorded_at::timestamp as recorded_at,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            mattermost_enabled::boolean as mattermost_enabled,
            uuid::varchar as uuid,
            edition::varchar as edition,
            hostname::varchar as hostname,
            host_id::number as host_id,
            license_trial::boolean as license_trial,
            source_license_id::number as source_license_id,
            installation_type::varchar as installation_type,
            license_plan::varchar as license_plan,
            database_adapter::varchar as database_adapter,
            database_version::varchar as database_version,
            git_version::varchar as git_version,
            gitlab_pages_enabled::boolean as gitlab_pages_enabled,
            gitlab_pages_version::varchar as gitlab_pages_version,
            container_registry_enabled::boolean as container_registry_enabled,
            elasticsearch_enabled::boolean as elasticsearch_enabled,
            geo_enabled::boolean as geo_enabled,
            gitlab_shared_runners_enabled::boolean as gitlab_shared_runners_enabled,
            gravatar_enabled::boolean as gravatar_enabled,
            ldap_enabled::boolean as ldap_enabled,
            omniauth_enabled::boolean as omniauth_enabled,
            reply_by_email_enabled::boolean as reply_by_email_enabled,
            signup_enabled::boolean as signup_enabled,
            -- web_ide_commits // was implemented as both a column and in `counts`
            prometheus_metrics_enabled::boolean as prometheus_metrics_enabled,
            parse_json(usage_activity_by_stage) as usage_activity_by_stage,
            parse_json(
                usage_activity_by_stage_monthly
            ) as usage_activity_by_stage_monthly,
            gitaly_clusters::number as gitaly_clusters,
            gitaly_version::varchar as gitaly_version,
            gitaly_servers::number as gitaly_servers,
            gitaly_filesystems::varchar as gitaly_filesystems,
            gitpod_enabled::varchar as gitpod_enabled,
            parse_json(object_store) as object_store,
            dependency_proxy_enabled::boolean as is_dependency_proxy_enabled,
            recording_ce_finished_at::timestamp as recording_ce_finished_at,
            recording_ee_finished_at::timestamp as recording_ee_finished_at,
            parse_json(stats) as stats_used,
            stats_used as counts,
            ingress_modsecurity_enabled::boolean as is_ingress_modsecurity_enabled,
            parse_json(topology) as topology,
            grafana_link_enabled::boolean as is_grafana_link_enabled,
            parse_json(analytics_unique_visits) as analytics_unique_visits,
            raw_usage_data_id::integer as raw_usage_data_id,
            container_registry_vendor::varchar as container_registry_vendor,
            container_registry_version::varchar as container_registry_version,
            raw_usage_data_payload_reconstructed
        from raw_usage_data_payload

    )

select *
from renamed
order by updated_at
