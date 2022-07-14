{{ config(tags=["mnpi_exception"]) }}

{{
    config(
        {
            "materialized": "table",
            "schema": "common_mart_product",
            "unique_key": "primary_key",
        }
    )
}}

{{
    simple_cte(
        [
            ("estimated_value", "mart_monthly_counter_adoption"),
            ("fct_usage_ping_payload", "fct_usage_ping_payload"),
            ("fct_monthly_usage_data", "fct_monthly_usage_data"),
        ]
    )
}},
smau as (

    select
        ping_created_month,
        clean_metrics_name,
        edition,
        product_tier,
        group_name,
        stage_name,
        section_name,
        is_smau,
        is_gmau,
        is_paid_gmau,
        is_umau,
        usage_ping_delivery_type,
        sum(monthly_metric_value) as monthly_metric_value_sum
    from fct_monthly_usage_data
    inner join
        fct_usage_ping_payload
        on fct_monthly_usage_data.dim_usage_ping_id
        = fct_usage_ping_payload.dim_usage_ping_id
    where is_smau = true {{ dbt_utils.group_by(n=12) }}



),
smau_joined as (

    select
        smau.*,
        usage_ping_delivery_type as delivery,
        'SMAU' as xmau_level,
        product_tier not in ('Core', 'CE')
        and usage_ping_delivery_type = 'Self-Managed' as is_paid,
        coalesce(
            estimated_value.pct_subscriptions_with_counters, 1
        ) as pct_subscriptions_with_counters
    from smau
    left join
        estimated_value
        on estimated_value.is_smau
        and smau.usage_ping_delivery_type = 'Self-Managed'
        and smau.ping_created_month = estimated_value.reporting_month
        and smau.stage_name = estimated_value.stage_name
        and smau.section_name = estimated_value.section_name
        and smau.edition = estimated_value.edition

),
umau as (

    select
        ping_created_month,
        clean_metrics_name,
        edition,
        product_tier,
        group_name,
        stage_name,
        section_name,
        is_smau,
        is_gmau,
        is_paid_gmau,
        is_umau,
        usage_ping_delivery_type,
        sum(monthly_metric_value) as monthly_metric_value_sum
    from fct_monthly_usage_data
    inner join
        fct_usage_ping_payload
        on fct_monthly_usage_data.dim_usage_ping_id
        = fct_usage_ping_payload.dim_usage_ping_id
    where is_umau = true {{ dbt_utils.group_by(n=12) }}



),
umau_joined as (

    select
        umau.*,
        usage_ping_delivery_type as delivery,
        'UMAU' as xmau_level,
        product_tier not in ('Core', 'CE')
        and usage_ping_delivery_type = 'Self-Managed' as is_paid,
        coalesce(
            estimated_value.pct_subscriptions_with_counters, 1
        ) as pct_subscriptions_with_counters
    from umau
    left join
        estimated_value
        on estimated_value.is_umau
        and umau.usage_ping_delivery_type = 'Self-Managed'
        and umau.ping_created_month = estimated_value.reporting_month
        and umau.edition = estimated_value.edition

),
instance_gmau as (

    select
        ping_created_month,
        clean_metrics_name,
        fct_monthly_usage_data.host_name,
        fct_monthly_usage_data.dim_instance_id,
        edition,
        product_tier,
        group_name,
        stage_name,
        section_name,
        is_smau,
        is_gmau,
        is_paid_gmau,
        is_umau,
        usage_ping_delivery_type,
        max(monthly_metric_value) as monthly_metric_value
    from fct_monthly_usage_data
    inner join
        fct_usage_ping_payload
        on fct_monthly_usage_data.dim_usage_ping_id
        = fct_usage_ping_payload.dim_usage_ping_id
    where is_gmau = true {{ dbt_utils.group_by(n=14) }}



),
gmau as (

    select
        ping_created_month,
        clean_metrics_name,
        edition,
        product_tier,
        group_name,
        stage_name,
        section_name,
        is_smau,
        is_gmau,
        is_paid_gmau,
        is_umau,
        usage_ping_delivery_type,
        sum(monthly_metric_value) as monthly_metric_value_sum
    from instance_gmau
    where is_gmau = true {{ dbt_utils.group_by(n=12) }}

),
gmau_joined as (

    select
        gmau.*,
        usage_ping_delivery_type as delivery,
        'GMAU' as xmau_level,
        product_tier not in ('Core', 'CE')
        and usage_ping_delivery_type = 'Self-Managed' as is_paid,
        coalesce(
            max(estimated_value.pct_subscriptions_with_counters), 1
        ) as pct_subscriptions_with_counters
    from gmau
    left join
        estimated_value
        on estimated_value.is_gmau
        and gmau.usage_ping_delivery_type = 'Self-Managed'
        and gmau.ping_created_month = estimated_value.reporting_month
        and gmau.stage_name = estimated_value.stage_name
        and gmau.group_name = estimated_value.group_name
        and gmau.section_name = estimated_value.section_name
        and gmau.edition = estimated_value.edition
        {{ dbt_utils.group_by(n=15) }}

),
xmau as (

    select *
    from gmau_joined

    union

    select *
    from smau_joined

    union

    select *
    from umau_joined

),
estimated_monthly_metric_value_sum as (

    select
        ping_created_month::date as reporting_month,
        delivery,
        xmau_level,
        is_smau,
        section_name,
        stage_name,
        group_name,
        iff(delivery = 'SaaS', delivery, product_tier) as product_tier,
        iff(delivery = 'SaaS', delivery, edition) as edition,
        'version' as data_source,
        sum(monthly_metric_value_sum) as recorded_monthly_metric_value_sum,
        sum(monthly_metric_value_sum)
        / max(pct_subscriptions_with_counters) as estimated_monthly_metric_value_sum
    from xmau {{ dbt_utils.group_by(n=10) }}

),
combined as (

    select
        reporting_month,
        section_name,
        stage_name,
        group_name,
        product_tier,
        xmau_level,
        iff(delivery = 'Self-Managed', 'Recorded Self-Managed', delivery) as breakdown,
        delivery,
        edition,
        sum(recorded_monthly_metric_value_sum) as recorded_monthly_metric_value_sum,
        -- this is expected as the breakdown is for Recorded Self-Managed and Saas
        -- Estimated Uplift being calculated in the next unioned table
        sum(recorded_monthly_metric_value_sum) as estimated_monthly_metric_value_sum
    from estimated_monthly_metric_value_sum {{ dbt_utils.group_by(n=9) }}

    union

    select
        reporting_month,
        section_name,
        stage_name,
        group_name,
        product_tier,
        xmau_level,
        'Estimated Self-Managed Uplift' as breakdown,
        delivery,
        edition,
        0 as recorded_monthly_metric_value_sum,
        -- calculating Estimated Uplift here
        sum(
            estimated_monthly_metric_value_sum - recorded_monthly_metric_value_sum
        ) as estimated_monthly_metric_value_sum
    from estimated_monthly_metric_value_sum
    where delivery = 'Self-Managed' {{ dbt_utils.group_by(n=9) }}

)

select *
from combined
