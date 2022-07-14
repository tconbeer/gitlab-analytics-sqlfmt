{{ config(tags=["product", "mnpi_exception"], materialized="table") }}

{{
    simple_cte(
        [
            ("dim_date", "dim_date"),
            ("prep_service_ping_instance", "prep_service_ping_instance"),
        ]
    )
}},
usage_data_w_date as (
    select prep_service_ping_instance.*, dim_date.date_id as dim_service_ping_date_id
    from prep_service_ping_instance
    left join dim_date on to_date(ping_created_at) = dim_date.date_day

),
last_ping_of_month_flag as (

    select distinct
        usage_data_w_date.id as id,
        usage_data_w_date.dim_service_ping_date_id as dim_service_ping_date_id,
        usage_data_w_date.uuid as uuid,
        usage_data_w_date.host_id as host_id,
        usage_data_w_date.ping_created_at::timestamp(0) as ping_created_at,
        dim_date.first_day_of_month as first_day_of_month,
        true as last_ping_of_month_flag
    from usage_data_w_date
    inner join dim_date on usage_data_w_date.dim_service_ping_date_id = dim_date.date_id
    qualify
        row_number() over (
            partition by
                usage_data_w_date.uuid,
                usage_data_w_date.host_id,
                dim_date.first_day_of_month
            order by ping_created_at desc
        )
        = 1

),
fct_w_month_flag as (

    select
        usage_data_w_date.*,
        last_ping_of_month_flag.last_ping_of_month_flag as last_ping_of_month_flag
    from usage_data_w_date
    left join
        last_ping_of_month_flag on usage_data_w_date.id = last_ping_of_month_flag.id

),
final as (

    select distinct
        dim_service_ping_instance_id as dim_service_ping_instance_id,
        dim_service_ping_date_id as dim_service_ping_date_id,
        dim_host_id as dim_host_id,
        dim_instance_id as dim_instance_id,
        dim_installation_id as dim_installation_id,
        to_date(ping_created_at) as ping_created_at,
        to_date(
            dateadd('days', -28, ping_created_at)
        ) as ping_created_at_28_days_earlier,
        to_date(date_trunc('YEAR', ping_created_at)) as ping_created_at_year,
        to_date(date_trunc('MONTH', ping_created_at)) as ping_created_at_month,
        to_date(date_trunc('WEEK', ping_created_at)) as ping_created_at_week,
        to_date(date_trunc('DAY', ping_created_at)) as ping_created_at_date,
        ip_address_hash as ip_address_hash,
        version as version,
        instance_user_count as instance_user_count,
        license_md5 as license_md5,
        historical_max_users as historical_max_users,
        license_user_count as license_user_count,
        license_starts_at as license_starts_at,
        license_expires_at as license_expires_at,
        license_add_ons as license_add_ons,
        recorded_at as recorded_at,
        updated_at as updated_at,
        mattermost_enabled as mattermost_enabled,
        main_edition as ping_edition,
        hostname as host_name,
        product_tier as product_tier,
        license_trial as is_trial,
        source_license_id as source_license_id,
        installation_type as installation_type,
        license_plan as license_plan,
        database_adapter as database_adapter,
        database_version as database_version,
        git_version as git_version,
        gitlab_pages_enabled as gitlab_pages_enabled,
        gitlab_pages_version as gitlab_pages_version,
        container_registry_enabled as container_registry_enabled,
        elasticsearch_enabled as elasticsearch_enabled,
        geo_enabled as geo_enabled,
        gitlab_shared_runners_enabled as gitlab_shared_runners_enabled,
        gravatar_enabled as gravatar_enabled,
        ldap_enabled as ldap_enabled,
        omniauth_enabled as omniauth_enabled,
        reply_by_email_enabled as reply_by_email_enabled,
        signup_enabled as signup_enabled,
        prometheus_metrics_enabled as prometheus_metrics_enabled,
        usage_activity_by_stage as usage_activity_by_stage,
        usage_activity_by_stage_monthly as usage_activity_by_stage_monthly,
        gitaly_clusters as gitaly_clusters,
        gitaly_version as gitaly_version,
        gitaly_servers as gitaly_servers,
        gitaly_filesystems as gitaly_filesystems,
        gitpod_enabled as gitpod_enabled,
        object_store as object_store,
        is_dependency_proxy_enabled as is_dependency_proxy_enabled,
        recording_ce_finished_at as recording_ce_finished_at,
        recording_ee_finished_at as recording_ee_finished_at,
        is_ingress_modsecurity_enabled as is_ingress_modsecurity_enabled,
        topology as topology,
        is_grafana_link_enabled as is_grafana_link_enabled,
        analytics_unique_visits as analytics_unique_visits,
        raw_usage_data_id as raw_usage_data_id,
        container_registry_vendor as container_registry_vendor,
        container_registry_version as container_registry_version,
        iff(
            license_expires_at >= ping_created_at or license_expires_at is null,
            ping_edition,
            'EE Free'
        ) as cleaned_edition,
        regexp_replace(nullif(version, ''), '[^0-9.]+') as cleaned_version,
        iff(version ilike '%-pre', true, false) as version_is_prerelease,
        split_part(cleaned_version, '.', 1)::number as major_version,
        split_part(cleaned_version, '.', 2)::number as minor_version,
        major_version || '.' || minor_version as major_minor_version,
        major_version * 100 + minor_version as major_minor_version_id,
        case
            when uuid = 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f'
            then 'SaaS'
            else 'Self-Managed'
        end as service_ping_delivery_type,
        case
            when service_ping_delivery_type = 'SaaS'
            then true
            when installation_type = 'gitlab-development-kit'
            then true
            when hostname = 'gitlab.com'
            then true
            when hostname ilike '%.gitlab.com'
            then true
            else false
        end as is_internal,
        case
            when hostname ilike 'staging.%'
            then true
            when hostname in ('staging.gitlab.com', 'dr.gitlab.com')
            then true
            else false
        end as is_staging,
        case
            when last_ping_of_month_flag = true then true else false
        end as is_last_ping_of_month
    from fct_w_month_flag

)

{{
    dbt_audit(
        cte_ref="final",
        created_by="@icooper-acp",
        updated_by="@icooper-acp",
        created_date="2022-03-08",
        updated_date="2022-03-11",
    )
}}
