with
    source as (select * from {{ source("snowflake_imports", "usage_ping_payloads") }}),
    parsed as (

        select
            jsontext['active_user_count']::number as active_user_count,
            jsontext['avg_cycle_analytics']::variant as avg_cycle_analytics,
            jsontext['container_registry_enabled']::boolean
            as is_container_registry_enabled,
            jsontext['counts']::variant as counts,
            jsontext['database']['adapter']::varchar as database_adapter,
            jsontext['database']['version']::varchar as database_version,
            jsontext['edition']::varchar as edition,
            jsontext['elasticsearch_enabled']::boolean as is_elasticsearch_enabled,
            jsontext['geo_enabled']::boolean as is_geo_enabled,
            jsontext['gitaly']['filesystems']::varchar as gitaly_filesystems,
            jsontext['gitaly']['servers']::number as gitaly_servers,
            jsontext['gitaly']['version']::varchar as gitaly_version,
            jsontext['gitlab_pages']['enabled']::boolean as is_gitlab_pages_enabled,
            jsontext['gitlab_pages']['version']::varchar as gitlab_pages_version,
            jsontext['gitlab_shared_runners_enabled']::boolean
            as is_gitlab_shared_runners_enabled,
            coalesce(
                jsontext['git_version']::varchar, jsontext['git']['version']::varchar
            ) as git_version,
            jsontext['gravatar_enabled']::boolean as is_gravatar_enabled,
            jsontext['historical_max_users']::number as historical_max_users,
            jsontext['hostname']::varchar as hostname,
            jsontext['installation_type']::varchar as installation_type,
            jsontext['ldap_enabled']::boolean as is_ldap_enabled,
            jsontext['licensee']::variant as licensee,
            jsontext['license_add_ons']::variant as license_add_ons,
            jsontext['license_expires_at']::timestamp as license_expires_at,
            jsontext['license_md5']::varchar as license_md5,
            jsontext['license_plan']::varchar as license_plan,
            jsontext['license_starts_at']::timestamp as license_starts_at,
            jsontext['license_trial']::boolean as is_license_trial,
            jsontext['license_user_count']::number as license_user_count,
            jsontext['mattermost_enabled']::boolean as is_mattermost_enabled,
            jsontext['omniauth_enabled']::boolean as is_omniauth_enabled,
            jsontext['prometheus_metrics_enabled']::boolean
            as is_prometheus_metrics_enabled,
            jsontext['recorded_at']::timestamp as recorded_at,
            jsontext['reply_by_email_enabled']::boolean as is_reply_by_email_enabled,
            jsontext['signup_enabled']::boolean as is_signup_enabled,
            jsontext['usage_activity_by_stage']::variant as usage_activity_by_stage,
            jsontext['uuid']::varchar as uuid,
            jsontext['version']::varchar as version,
            jsontext['license_trial_ends_on']::timestamp as license_trial_ends_on,
            jsontext['web_ide_clientside_preview_enabled']::boolean
            as is_web_ide_clientside_preview_enabled,
            jsontext['ingress_modsecurity_enabled']::boolean
            as is_ingress_modsecurity_enabled,
            jsontext['dependency_proxy_enabled']::boolean as is_dependency_proxy_enabled
        from source

    )

select *
from parsed
