{{ config(tags=["product", "mnpi_exception"]) }}

{%- set settings_columns = dbt_utils.get_column_values(
    table=ref("prep_usage_ping_metrics_setting"),
    column="metrics_path",
    max_records=1000,
    default=[""],
) %}

{{
    simple_cte(
        [
            ("prep_usage_data_flattened", "prep_usage_data_flattened"),
            ("prep_usage_ping_metrics_setting", "prep_usage_ping_metrics_setting"),
        ]
    )
}},
usage_ping as (

    select
        id as dim_usage_ping_id,
        created_at::timestamp(0) as ping_created_at,
        *,
        {{ nohash_sensitive_columns("version_usage_data_source", "source_ip") }}
        as ip_address_hash,
        object_construct(
            {% for column in columns %}
            '{{ column.name | lower }}',
            {{ column.name | lower }}
            {% if not loop.last %}, {% endif %}
            {% endfor %}
        ) as raw_usage_data_payload_reconstructed
    from {{ ref("version_usage_data_source") }}

),
settings_data as (

    select
        dim_usage_ping_id,
        {% for column in settings_columns %}
        max(
            iff(
                prep_usage_data_flattened.metrics_path = '{{column}}',
                metric_value,
                null
            )
        ) as {{ column | replace(".", "_") }}
        {{ "," if not loop.last }}
        {% endfor %}
    from prep_usage_data_flattened
    inner join
        prep_usage_ping_metrics_setting
        on prep_usage_data_flattened.metrics_path
        = prep_usage_ping_metrics_setting.metrics_path
    group by 1

),
renamed as (

    select
        dim_usage_ping_id,
        container_registry_server_version as container_registry_server_version,
        database_adapter as database_adapter,
        database_version as database_version,
        git_version as git_version,
        gitaly_version as gitaly_version,
        gitlab_pages_version as gitlab_pages_version,
        container_registry_enabled as is_container_registry_enabled,
        dependency_proxy_enabled as is_dependency_proxy_enabled,
        elasticsearch_enabled as is_elasticsearch_enabled,
        geo_enabled as is_geo_enabled,
        gitlab_pages_enabled as is_gitlab_pages_enabled,
        gitpod_enabled as is_gitpod_enabled,
        grafana_link_enabled as is_grafana_link_enabled,
        gravatar_enabled as is_gravatar_enabled,
        ingress_modsecurity_enabled as is_ingress_modsecurity_enabled,
        instance_auto_devops_enabled as is_instance_auto_devops_enabled,
        ldap_enabled as is_ldap_enabled,
        mattermost_enabled as is_mattermost_enabled,
        object_store_artifacts_enabled as is_object_store_artifacts_enabled,
        object_store_artifacts_object_store_enabled
        as is_object_store_artifacts_object_store_enabled,
        object_store_external_diffs_enabled as is_object_store_external_diffs_enabled,
        object_store_external_diffs_object_store_enabled
        as is_object_store_external_diffs_object_store_enabled,
        object_store_lfs_enabled as is_object_store_lfs_enabled,
        object_store_packages_enabled as is_object_store_packages_enabled,
        object_store_packages_object_store_enabled
        as is_object_store_packages_object_store_enabled,
        object_store_uploads_object_store_enabled
        as is_object_store_uploads_object_store_enabled,
        omniauth_enabled as is_omniauth_enabled,
        prometheus_enabled as is_prometheus_enabled,
        prometheus_metrics_enabled as is_prometheus_metrics_enabled,
        reply_by_email_enabled as is_reply_by_email_enabled,
        settings_ldap_encrypted_secrets_enabled
        as is_settings_ldap_encrypted_secrets_enabled,
        signup_enabled as is_signup_enabled,
        usage_activity_by_stage_manage_group_saml_enabled
        as is_usage_activity_by_stage_manage_group_saml_enabled,
        usage_activity_by_stage_manage_ldap_admin_sync_enabled
        as is_usage_activity_by_stage_manage_ldap_admin_sync_enabled,
        usage_activity_by_stage_manage_ldap_group_sync_enabled
        as is_usage_activity_by_stage_manage_ldap_group_sync_enabled,
        usage_activity_by_stage_monthly_manage_group_saml_enabled
        as is_usage_activity_by_stage_monthly_manage_group_saml_enabled,
        usage_activity_by_stage_monthly_manage_ldap_admin_sync_enabled
        as is_usage_activity_by_stage_monthly_manage_ldap_admin_sync_enabled,
        usage_activity_by_stage_monthly_manage_ldap_group_sync_enabled
        as is_usage_activity_by_stage_monthly_manage_ldap_group_sync_enabled,
        web_ide_clientside_preview_enabled as is_web_ide_clientside_preview_enabled,
        object_store_artifacts_object_store_background_upload
        as object_store_artifacts_object_store_background_upload,
        object_store_artifacts_object_store_direct_upload
        as object_store_artifacts_object_store_direct_upload,
        object_store_external_diffs_object_store_background_upload
        as object_store_external_diffs_object_store_background_upload,
        object_store_external_diffs_object_store_direct_upload
        as object_store_external_diffs_object_store_direct_upload,
        object_store_lfs_object_store_background_upload
        as object_store_lfs_object_store_background_upload,
        object_store_lfs_object_store_direct_upload
        as object_store_lfs_object_store_direct_upload,
        object_store_packages_object_store_background_upload
        as object_store_packages_object_store_background_upload,
        object_store_packages_object_store_direct_upload
        as object_store_packages_object_store_direct_upload,
        object_store_uploads_object_store_background_upload
        as object_store_uploads_object_store_background_upload,
        object_store_uploads_object_store_direct_upload
        as object_store_uploads_object_store_direct_upload,
        settings_gitaly_apdex as settings_gitaly_apdex,
        settings_operating_system as settings_operating_system,
        topology as topology
    from settings_data

)

select *
from renamed
