{{ config(tags=["mnpi_exception"]) }}

{{
    simple_cte(
        [
            ("mart_monthly_product_usage", "mart_monthly_product_usage"),
            (
                "mart_usage_ping_counters_statistics",
                "mart_usage_ping_counters_statistics",
            ),
            ("dim_gitlab_releases", "dim_gitlab_releases"),
            (
                "mart_paid_subscriptions_monthly_usage_ping_optin",
                "mart_paid_subscriptions_monthly_usage_ping_optin",
            ),
            (
                "wk_usage_ping_monthly_events_distribution_by_version",
                "wk_usage_ping_monthly_events_distribution_by_version",
            ),
        ]
    )
}}

,
cte_joined as (

    select
        reporting_month,
        stage_name,
        iff(
            main_edition = 'CE',
            main_edition,
            iff(ping_product_tier = 'Core', 'EE - Core', 'EE - Paid')
        ) as reworked_main_edition,
        datediff(
            'month', date_trunc('month', release_date), reporting_month
        ) as months_since_release,
        sum(monthly_metric_value) as month_metric_value_sum
    from mart_monthly_product_usage
    left join
        mart_usage_ping_counters_statistics
        on mart_monthly_product_usage.main_edition
        = mart_usage_ping_counters_statistics.edition
        and mart_monthly_product_usage.metrics_path
        = mart_usage_ping_counters_statistics.metrics_path
    left join
        dim_gitlab_releases
        on dim_gitlab_releases.major_minor_version
        = mart_usage_ping_counters_statistics.first_version_with_counter
    where is_smau = true and delivery = 'Self-Managed'
    group by 1, 2, 3, 4

),
pct_of_instances as (

    select
        cte_joined.reporting_month,
        month_metric_value_sum,
        stage_name,
        cte_joined.reworked_main_edition,
        sum(
            case
                when distrib.months_since_release <= cte_joined.months_since_release
                then total_counts
            end
        ) / sum(total_counts) as pct_of_instances
    from cte_joined
    left join
        wk_usage_ping_monthly_events_distribution_by_version as distrib
        on cte_joined.reporting_month = distrib.reporting_month
        and distrib.reworked_main_edition = cte_joined.reworked_main_edition
    group by 1, 2, 3, 4

),
averaged as (select * from pct_of_instances),
opt_in_rate as (

    select reporting_month, avg(has_sent_payloads::integer) as opt_in_rate
    from mart_paid_subscriptions_monthly_usage_ping_optin
    group by 1
)

select
    pct_of_instances.reporting_month::date as reporting_month,
    stage_name,
    reworked_main_edition,
    iff(reworked_main_edition = 'CE', 'CE', 'EE') as main_edition,
    pct_of_instances,
    month_metric_value_sum,
    month_metric_value_sum / pct_of_instances / opt_in_rate as estimated_xmau
from pct_of_instances
left join opt_in_rate on pct_of_instances.reporting_month = opt_in_rate.reporting_month
