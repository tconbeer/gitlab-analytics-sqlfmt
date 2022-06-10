{{ config(tags=["product", "mnpi_exception"], materialized="table") }}

{{
    simple_cte(
        [
            (
                "mart_service_ping_instance_metric_28_day",
                "mart_service_ping_instance_metric_28_day",
            ),
            ("mart_pct", "rpt_service_ping_instance_metric_adoption_monthly_all"),
        ]
    )
}}

-- Fact data from mart_service_ping_instance_metric_28_day, bringing in only last ping
-- of months which are valid
,
fact as (

    select
        metrics_path as metrics_path,
        ping_created_at_month as reporting_month,
        service_ping_delivery_type as service_ping_delivery_type,
        ping_edition as ping_edition,
        ping_product_tier as ping_product_tier,
        ping_edition_product_tier as ping_edition_product_tier,
        stage_name as stage_name,
        section_name as section_name,
        group_name as group_name,
        is_smau as is_smau,
        is_gmau as is_gmau,
        is_paid_gmau as is_paid_gmau,
        is_umau as is_umau,
        sum(metric_value) as actual_usage
    from mart_service_ping_instance_metric_28_day
    where
        is_last_ping_of_month = true
        and has_timed_out = false
        and metric_value is not null
        {{ dbt_utils.group_by(n=13) }}

-- Join in adoption percentages to determine what to estimate for self managed
),
sm_joined_counts_w_percentage as (

    select
        fact.*,
        mart_pct.reporting_count as reporting_count,
        mart_pct.no_reporting_count as no_reporting_count,
        mart_pct.percent_reporting as percent_reporting,
        mart_pct.estimation_grain as estimation_grain
    from fact
    inner join
        mart_pct
        on fact.reporting_month = mart_pct.reporting_month
        and fact.metrics_path = mart_pct.metrics_path
    where service_ping_delivery_type = 'Self-Managed'

-- No need to join in SaaS, it is what it is (all reported and accurate)
),
saas_joined_counts_w_percentage as (

    select
        fact.*,
        1 as reporting_count,
        0 as no_reporting_count,
        1 as percent_reporting,
        'SaaS' as estimation_grain
    from fact
    where service_ping_delivery_type = 'SaaS'

-- Union SaaS and Self Managed tables
),
joined_counts_w_percentage as (

    select *
    from saas_joined_counts_w_percentage

    UNION ALL

    select *
    from sm_joined_counts_w_percentage

-- Format output
),
final as (

    select
        {{
            dbt_utils.surrogate_key(
                [
                    "reporting_month",
                    "metrics_path",
                    "estimation_grain",
                    "ping_edition_product_tier",
                    "service_ping_delivery_type",
                ]
            )
        }} as rpt_service_ping_instance_metric_estimated_monthly_id,
        -- identifiers
        metrics_path as metrics_path,
        reporting_month as reporting_month,
        service_ping_delivery_type as service_ping_delivery_type,
        -- ping attributes
        ping_edition as ping_edition,
        ping_product_tier as ping_product_tier,
        ping_edition_product_tier as ping_edition_product_tier,
        -- metric attributes
        stage_name as stage_name,
        section_name as section_name,
        group_name as group_name,
        is_smau as is_smau,
        is_gmau as is_gmau,
        is_paid_gmau as is_paid_gmau,
        is_umau as is_umau,
        -- fct info
        reporting_count as reporting_count,
        no_reporting_count as no_reporting_count,
        percent_reporting as percent_reporting,
        estimation_grain as estimation_grain,
        {{ usage_estimation("actual_usage", "percent_reporting") }}
        as total_usage_estimated,
        total_usage_estimated - actual_usage as estimated_usage,
        actual_usage as actual_usage
    from joined_counts_w_percentage

)

{{
    dbt_audit(
        cte_ref="final",
        created_by="@icooper-acp",
        updated_by="@icooper-acp",
        created_date="2022-04-20",
        updated_date="2022-04-20",
    )
}}
