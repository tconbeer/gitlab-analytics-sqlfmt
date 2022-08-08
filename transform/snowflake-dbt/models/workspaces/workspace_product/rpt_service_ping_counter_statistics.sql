{{ config(tags=["product", "mnpi_exception"], materialized="table") }}

-- find min and max version for each metric
{{
    simple_cte(
        [
            (
                "mart_service_ping_instance_metric",
                "mart_service_ping_instance_metric",
            ),
            ("dim_gitlab_releases", "dim_gitlab_releases"),
        ]
    )
}},
transformed as (

    select distinct
        {{ dbt_utils.surrogate_key(["metrics_path", "ping_edition"]) }}
        as rpt_service_ping_counter_statistics_id,
        metrics_path as metrics_path,
        ping_edition as ping_edition,
        first_value(mart_service_ping_instance_metric.major_minor_version) over (
            partition by metrics_path order by release_date asc
        ) as first_version_with_counter,
        min(mart_service_ping_instance_metric.major_version) over (
            partition by metrics_path
        ) as first_major_version_with_counter,
        first_value(mart_service_ping_instance_metric.minor_version) over (
            partition by metrics_path order by release_date asc
        ) as first_minor_version_with_counter,
        last_value(mart_service_ping_instance_metric.major_minor_version) over (
            partition by metrics_path order by release_date asc
        ) as last_version_with_counter,
        max(mart_service_ping_instance_metric.major_version) over (
            partition by metrics_path
        ) as last_major_version_with_counter,
        last_value(mart_service_ping_instance_metric.minor_version) over (
            partition by metrics_path order by release_date asc
        ) as last_minor_version_with_counter,
        count(distinct dim_installation_id) over (
            partition by metrics_path
        ) as dim_installation_count,
        iff(
            first_version_with_counter = last_version_with_counter, true, false
        ) as diff_version_flag
    from mart_service_ping_instance_metric
    left join
        dim_gitlab_releases
        on mart_service_ping_instance_metric.major_minor_version
        = dim_gitlab_releases.major_minor_version
    where  -- TRY_TO_DECIMAL(metric_value::TEXT) > 0
        -- Removing SaaS
        dim_instance_id != 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f'
        -- Removing pre-releases
        and version_is_prerelease = false

)

{{
    dbt_audit(
        cte_ref="transformed",
        created_by="@icooper-acp",
        updated_by="@icooper-acp",
        created_date="2022-04-07",
        updated_date="2022-04-15",
    )
}}
