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
            ("dim_date", "dim_date"),
            ("estimated_value", "mart_monthly_counter_adoption"),
            ("fct_usage_ping_payload", "fct_usage_ping_payload"),
            ("fct_monthly_usage_data", "fct_monthly_usage_data"),
            ("fct_daily_event_400", "fct_daily_event_400"),
            ("map_saas_event_to_gmau", "map_saas_event_to_gmau"),
            ("map_saas_event_to_smau", "map_saas_event_to_smau"),
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
    where
        is_smau = true
        and product_tier <> 'Core'
        and usage_ping_delivery_type = 'Self-Managed'
        {{ dbt_utils.group_by(n=12) }}



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
saas_smau as (

    select
        first_day_of_month as reporting_month,
        'SaaS' as delivery,
        null as section_name,
        stage_name,
        null as group_name,
        'SMAU' as xmau_level,
        'SaaS' as product_tier,
        'SaaS' as edition,
        count(distinct dim_user_id) as recorded_monthly_metric_value_sum,
        recorded_monthly_metric_value_sum as estimated_monthly_metric_value_sum

    from fct_daily_event_400
    inner join
        map_saas_event_to_smau
        on fct_daily_event_400.event_name = map_saas_event_to_smau.event_name
    inner join
        dim_date
        on fct_daily_event_400.event_created_date = dim_date.date_day
        and datediff('day', event_created_date, last_day_of_month) < 28
    where
        fct_daily_event_400.dim_plan_id_at_event_date <> 34
        {{ dbt_utils.group_by(n=8) }}


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
    where
        is_umau = true
        and product_tier <> 'Core'
        and usage_ping_delivery_type = 'Self-Managed'
        {{ dbt_utils.group_by(n=12) }}



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
    where
        (
            -- if a specific paid_gmau metric has beeen creeated we don't need to
            -- exclude SaaS
            (is_paid_gmau = true and usage_ping_delivery_type = 'Self-Managed')
            or (is_paid_gmau = true and is_gmau = false)
        )
        and product_tier <> 'Core'

        {{ dbt_utils.group_by(n=14) }}



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
    where is_paid_gmau = true {{ dbt_utils.group_by(n=12) }}

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
        on estimated_value.is_paid_gmau
        and gmau.usage_ping_delivery_type = 'Self-Managed'
        and gmau.ping_created_month = estimated_value.reporting_month
        and gmau.stage_name = estimated_value.stage_name
        and gmau.group_name = estimated_value.group_name
        and gmau.section_name = estimated_value.section_name
        and gmau.edition = estimated_value.edition
        {{ dbt_utils.group_by(n=15) }}

),
saas_gmau as (

    select
        first_day_of_month as reporting_month,
        'SaaS' as delivery,
        null as section_name,
        stage_name,
        group_name,
        'GMAU' as xmau_level,
        'SaaS' as product_tier,
        'SaaS' as edition,
        count(distinct dim_user_id) as recorded_monthly_metric_value_sum,
        recorded_monthly_metric_value_sum as estimated_monthly_metric_value_sum

    from fct_daily_event_400
    inner join
        map_saas_event_to_gmau
        on fct_daily_event_400.event_name = map_saas_event_to_gmau.event_name
    inner join
        dim_date
        on fct_daily_event_400.event_created_date = dim_date.date_day
        and datediff('day', event_created_date, last_day_of_month) < 28
    where
        fct_daily_event_400.dim_plan_id_at_event_date <> 34
        {{ dbt_utils.group_by(n=8) }}

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

    union

    select
        reporting_month,
        section_name,
        stage_name,
        group_name,
        product_tier,
        xmau_level,
        'SaaS' as breakdown,
        delivery,
        edition,
        recorded_monthly_metric_value_sum,
        estimated_monthly_metric_value_sum
    from saas_gmau

    union

    select
        reporting_month,
        section_name,
        stage_name,
        group_name,
        product_tier,
        xmau_level,
        'SaaS' as breakdown,
        delivery,
        edition,
        recorded_monthly_metric_value_sum,
        estimated_monthly_metric_value_sum
    from saas_smau

)

select *
from combined
