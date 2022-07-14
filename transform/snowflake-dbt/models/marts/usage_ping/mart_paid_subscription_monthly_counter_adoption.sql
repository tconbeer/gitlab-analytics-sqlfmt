{{ config(tags=["mnpi_exception"]) }}

with
    self_managed_active_subscriptions as (

        select
            dim_date_id as date_id,
            dim_subscription_id as subscription_id,
            dim_product_detail_id as product_details_id,
            sum(mrr) as mrr,
            sum(quantity) as quantity
        from {{ ref("fct_mrr") }}
        where
            subscription_status in ('Active', 'Cancelled') {{ dbt_utils.group_by(n=3) }}

    ),
    dim_date as (

        select distinct date_id, first_day_of_month
        from {{ ref("dim_date") }}
        where first_day_of_month < current_date

    ),
    dim_product_detail as (select * from {{ ref("dim_product_detail") }}),
    active_subscriptions as (

        select *
        from {{ ref("dim_subscription") }}
        where subscription_status not in ('Draft', 'Expired')

    ),
    all_subscriptions as (select * from {{ ref("dim_subscription") }}),
    fct_payload as (select * from {{ ref("fct_usage_ping_payload") }}),
    dim_gitlab_releases as (select * from {{ ref("dim_gitlab_releases") }}),
    transformed as (

        select
            {{
                dbt_utils.surrogate_key(
                    [
                        "first_day_of_month",
                        "self_managed_active_subscriptions.subscription_id",
                    ]
                )
            }} as month_subscription_id,
            first_day_of_month as reporting_month,
            self_managed_active_subscriptions.subscription_id,
            active_subscriptions.subscription_name_slugify,
            active_subscriptions.subscription_start_date,
            active_subscriptions.subscription_end_date,
            max(fct_payload.dim_subscription_id) is not null as has_sent_payloads,
            count(distinct fct_payload.dim_usage_ping_id) as monthly_payload_counts,
            count(distinct host_name) as monthly_host_counts
        from self_managed_active_subscriptions
        inner join
            dim_product_detail
            on self_managed_active_subscriptions.product_details_id
            = dim_product_detail.dim_product_detail_id
            and product_delivery_type = 'Self-Managed'
        inner join
            dim_date on self_managed_active_subscriptions.date_id = dim_date.date_id
        left join
            active_subscriptions
            on self_managed_active_subscriptions.subscription_id
            = active_subscriptions.dim_subscription_id
        left join
            all_subscriptions
            on active_subscriptions.subscription_name_slugify
            = all_subscriptions.subscription_name_slugify
        left join
            fct_payload
            on all_subscriptions.dim_subscription_id = fct_payload.dim_subscription_id
            and dim_date.first_day_of_month
            = date_trunc('month', fct_payload.ping_created_at)
            {{ dbt_utils.group_by(n=6) }}

    ),
    latest_versions as (

        select distinct
            first_day_of_month as reporting_month,
            self_managed_active_subscriptions.subscription_id,
            active_subscriptions.subscription_name_slugify,
            first_value(major_minor_version) OVER (
                partition by
                    first_day_of_month, active_subscriptions.subscription_name_slugify
                order by ping_created_at desc
            ) as latest_major_minor_version
        from self_managed_active_subscriptions
        inner join
            dim_product_detail
            on self_managed_active_subscriptions.product_details_id
            = dim_product_detail.dim_product_detail_id
            and product_delivery_type = 'Self-Managed'
        inner join
            dim_date on self_managed_active_subscriptions.date_id = dim_date.date_id
        inner join
            active_subscriptions
            on self_managed_active_subscriptions.subscription_id
            = active_subscriptions.dim_subscription_id
        inner join
            all_subscriptions
            on active_subscriptions.subscription_name_slugify
            = all_subscriptions.subscription_name_slugify
        inner join
            fct_payload
            on all_subscriptions.dim_subscription_id = fct_payload.dim_subscription_id
            and first_day_of_month = date_trunc('month', fct_payload.ping_created_at)

    ),
    paid_subscriptions_monthly_usage_ping_optin as (

        select transformed.*, latest_versions.latest_major_minor_version
        from transformed
        left join
            latest_versions
            on transformed.reporting_month = latest_versions.reporting_month
            and transformed.subscription_name_slugify
            = latest_versions.subscription_name_slugify

    ),
    agg_total_subscriptions as (

        select
            reporting_month as agg_month,
            count(distinct subscription_name_slugify) as total_subscrption_count
        from paid_subscriptions_monthly_usage_ping_optin {{ dbt_utils.group_by(n=1) }}

    ),
    monthly_subscription_optin_counts as (

        select distinct
            paid_subscriptions_monthly_usage_ping_optin.reporting_month,
            latest_major_minor_version,
            major_version,
            minor_version,
            count(
                distinct subscription_name_slugify
            ) as major_minor_version_subscriptions,
            major_minor_version_subscriptions
            / max(total_subscrption_count) as pct_major_minor_version_subscriptions
        from paid_subscriptions_monthly_usage_ping_optin
        inner join
            dim_gitlab_releases as gitlab_releases
            on paid_subscriptions_monthly_usage_ping_optin.latest_major_minor_version
            = gitlab_releases.major_minor_version
        left join
            agg_total_subscriptions as agg
            on paid_subscriptions_monthly_usage_ping_optin.reporting_month
            = agg.agg_month
            {{ dbt_utils.group_by(n=4) }}

    ),
    section_metrics as (

        select *
        from {{ ref("sheetload_usage_ping_metrics_sections") }}
        where is_smau or is_gmau or clean_metrics_name = 'monthly_active_users_28_days'

    ),
    flattened_usage_data as (

        select distinct
            f.path as ping_name,
            iff(edition = 'CE', edition, 'EE') as edition,
            split_part(ping_name, '.', 1) as main_json_name,
            split_part(ping_name, '.', -1) as feature_name,
            replace(f.path, '.', '_') as full_ping_name,
            first_value(major_minor_version) OVER (
                partition by full_ping_name
                order by major_version asc, minor_version asc
            ) as first_version_with_counter
        from
            {{ ref("version_usage_data") }},
            lateral flatten(
                input => version_usage_data.raw_usage_data_payload, recursive => true) f



    ),
    counter_data as (

        select distinct
            first_value(major_version) OVER (
                partition by group_name order by release_date asc
            ) as major_version,
            first_value(minor_version) OVER (
                partition by group_name order by release_date asc
            ) as minor_version,
            first_value(date_trunc('month', release_date)) OVER (
                partition by group_name order by major_version asc, minor_version asc
            ) as release_month,
            stage_name,
            section_name,
            group_name,
            is_smau,
            is_gmau,
            first_value(major_minor_version) OVER (
                partition by group_name order by major_version asc, minor_version asc
            ) as first_version_with_counter,
            edition
        from flattened_usage_data
        inner join
            section_metrics
            on flattened_usage_data.ping_name = section_metrics.metrics_path
        left join
            dim_gitlab_releases as gitlab_releases
            on flattened_usage_data.first_version_with_counter
            = gitlab_releases.major_minor_version
        where release_date < current_date and (is_smau or is_gmau)

    ),
    date_spine as (

        select distinct first_day_of_month as reporting_month
        from {{ ref("date_details") }}
        where first_day_of_month < current_date and first_day_of_month >= '2018-01-01'

    ),
    date_joined as (

        select
            date_spine.reporting_month,
            first_version_with_counter,
            edition,
            stage_name,
            section_name,
            group_name,
            is_smau,
            is_gmau,
            sum(
                pct_major_minor_version_subscriptions
            ) as pct_subscriptions_with_counters
        from date_spine
        inner join counter_data on date_spine.reporting_month >= release_month
        left join
            monthly_subscription_optin_counts
            on date_spine.reporting_month
            = monthly_subscription_optin_counts.reporting_month
            and (
                counter_data.major_version
                < monthly_subscription_optin_counts.major_version
                or
                (
                    counter_data.major_version
                    = monthly_subscription_optin_counts.major_version
                    and counter_data.minor_version
                    <= monthly_subscription_optin_counts.minor_version
                )
            )
        where
            date_spine.reporting_month < date_trunc('month', current_date)
            {{ dbt_utils.group_by(n=8) }}

    )

select *
from date_joined
