{{ config(tags=["mnpi_exception"]) }}

with
    paid_subscriptions_monthly_usage_ping_optin as (

        select * from {{ ref("mart_paid_subscriptions_monthly_usage_ping_optin") }}

    ),
    gitlab_releases as (select * from {{ ref("dim_gitlab_releases") }}),
    agg_total_subscriptions as (

        select
            reporting_month as agg_month,
            count(distinct subscription_name_slugify) as total_subscrption_count
        from paid_subscriptions_monthly_usage_ping_optin
        group by 1

    ),
    monthly_subscription_optin_counts as (

        select
            paid_subscriptions_monthly_usage_ping_optin.reporting_month,
            latest_major_minor_version,
            major_version,
            minor_version,
            release_date,
            date_trunc('month', release_date) as release_month,
            count(
                distinct subscription_name_slugify
            ) as major_minor_version_subscriptions,
            major_minor_version_subscriptions
            / max(total_subscrption_count) as pct_major_minor_version_subscriptions
        from paid_subscriptions_monthly_usage_ping_optin
        inner join
            gitlab_releases
            on paid_subscriptions_monthly_usage_ping_optin.latest_major_minor_version
            = gitlab_releases.major_minor_version
        left join
            agg_total_subscriptions as agg
            on paid_subscriptions_monthly_usage_ping_optin.reporting_month
            = agg.agg_month
            {{ dbt_utils.group_by(n=6) }}

    ),
    section_metrics as (

        select * from {{ ref("sheetload_usage_ping_metrics_sections") }}

    ),
    flattened_usage_data as (select * from {{ ref("poc_prep_usage_data_flattened") }}),
    transformed_flattened as (

        select distinct
            metrics_path,
            iff(edition = 'CE', edition, 'EE') as edition,
            split_part(metrics_path, '.', 1) as main_json_name,
            split_part(metrics_path, '.', -1) as feature_name,
            replace(metrics_path, '.', '_') as full_metrics_path,
            first_value(flattened_usage_data.major_minor_version) OVER (
                partition by metrics_path, edition order by release_date asc
            ) as first_version_with_counter
        from flattened_usage_data
        left join
            gitlab_releases
            on flattened_usage_data.major_minor_version
            = gitlab_releases.major_minor_version
        where
            try_to_decimal(metric_value::text) > 0
            -- Removing SaaS
            and instance_id <> 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f'
            -- Removing pre-releases
            and version not like '%pre'

    ),
    counter_data as (

        select distinct
            first_value(major_version) OVER (
                partition by transformed_flattened.metrics_path, edition
                order by release_date asc
            ) as major_version,
            first_value(minor_version) OVER (
                partition by transformed_flattened.metrics_path, edition
                order by release_date asc
            ) as minor_version,
            first_value(date_trunc('month', release_date)) OVER (
                partition by transformed_flattened.metrics_path, edition
                order by release_date asc
            ) as release_month,
            transformed_flattened.metrics_path,
            stage_name,
            section_name,
            group_name,
            is_smau,
            is_gmau,
            is_umau,
            is_paid_gmau,
            edition,
            first_value(major_minor_version) OVER (
                partition by transformed_flattened.metrics_path, edition
                order by release_date asc
            ) as first_version_with_counter
        from transformed_flattened
        inner join
            section_metrics
            on transformed_flattened.metrics_path = section_metrics.metrics_path
        left join
            gitlab_releases
            on transformed_flattened.first_version_with_counter
            = gitlab_releases.major_minor_version
        where release_date < current_date

    ),
    date_spine as (

        select distinct first_day_of_month as reporting_month
        from {{ ref("date_details") }}
        where first_day_of_month >= '2018-01-01' and first_day_of_month < current_date

    ),
    date_joined as (

        select
            date_spine.reporting_month,
            first_version_with_counter,
            metrics_path,
            counter_data.edition,
            stage_name,
            section_name,
            group_name,
            is_smau,
            is_gmau,
            is_paid_gmau,
            is_umau,
            sum(
                pct_major_minor_version_subscriptions
            ) as pct_subscriptions_with_counters
        from date_spine
        inner join counter_data on date_spine.reporting_month >= release_month
        left join
            monthly_subscription_optin_counts
            on date_spine.reporting_month
            = monthly_subscription_optin_counts.reporting_month
            and counter_data.release_month
            <= monthly_subscription_optin_counts.release_month
            {{ dbt_utils.group_by(n=11) }}

    )

select *
from date_joined
