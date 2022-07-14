/* grain: one record per host per metric per month */
{{ config(tags=["mnpi_exception"]) }}

{{
    simple_cte(
        [
            ("flattened_usage_data", "prep_usage_data_flattened"),
            ("fct_usage_ping_payload", "fct_usage_ping_payload"),
            ("dim_gitlab_releases", "dim_gitlab_releases"),
        ]
    )
}},
transformed as (

    select distinct
        metrics_path,
        iff(fct_usage_ping_payload.edition = 'CE', edition, 'EE') as edition,
        split_part(metrics_path, '.', 1) as main_json_name,
        split_part(metrics_path, '.', -1) as feature_name,
        first_value(fct_usage_ping_payload.major_minor_version) OVER (
            partition by metrics_path order by release_date asc
        ) as first_version_with_counter,
        min(fct_usage_ping_payload.major_version) OVER (
            partition by metrics_path
        ) as first_major_version_with_counter,
        first_value(fct_usage_ping_payload.minor_version) OVER (
            partition by metrics_path order by release_date asc
        ) as first_minor_version_with_counter,
        last_value(fct_usage_ping_payload.major_minor_version) OVER (
            partition by metrics_path order by release_date asc
        ) as last_version_with_counter,
        max(fct_usage_ping_payload.major_version) OVER (
            partition by metrics_path
        ) as last_major_version_with_counter,
        last_value(fct_usage_ping_payload.minor_version) OVER (
            partition by metrics_path order by release_date asc
        ) as last_minor_version_with_counter,
        count(distinct dim_instance_id) OVER (
            partition by metrics_path
        ) as count_instances
    from flattened_usage_data
    left join
        fct_usage_ping_payload
        on flattened_usage_data.dim_usage_ping_id
        = fct_usage_ping_payload.dim_usage_ping_id
    left join
        dim_gitlab_releases
        on fct_usage_ping_payload.major_minor_version
        = dim_gitlab_releases.major_minor_version
    where
        try_to_decimal(metric_value::text) > 0
        -- Removing SaaS
        and dim_instance_id <> 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f'
        -- Removing pre-releases
        and version_is_prerelease = false

)

select *
from transformed
