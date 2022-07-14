{{ config(tags=["product", "mnpi_exception"]) }}

{{
    simple_cte(
        [
            ("dim_product_detail", "dim_product_detail"),
            ("prep_usage_ping_payload", "prep_usage_ping_payload"),
            (
                "map_usage_ping_active_subscription",
                "map_usage_ping_active_subscription",
            ),
            ("prep_charge", "prep_charge"),
        ]
    )
}},
dim_date as (

    select * from {{ ref("dim_date") }} where first_day_of_month < current_date

),
self_managed_active_subscriptions as (

    select
        dim_date.date_id as dim_date_id,
        prep_charge.dim_subscription_id,
        sum(prep_charge.mrr) as mrr,
        sum(prep_charge.quantity) as quantity
    from prep_charge
    inner join
        dim_date
        on prep_charge.effective_start_month <= dim_date.date_actual
        and (
            prep_charge.effective_end_month > dim_date.date_actual
            or prep_charge.effective_end_month is null
        )
        and dim_date.day_of_month = 1
    inner join
        dim_product_detail
        on prep_charge.dim_product_detail_id = dim_product_detail.dim_product_detail_id
        and product_delivery_type = 'Self-Managed'
    where
        subscription_status in ('Active', 'Cancelled')
        /* This excludes Education customers (charge name EDU or OSS) with free subscriptions.
         Pull in seats from Paid EDU Plans with no ARR */
        and (mrr != 0 or lower(prep_charge.rate_plan_charge_name) = 'max enrollment')
        {{ dbt_utils.group_by(n=2) }}

),
mau as (

    select *
    from {{ ref("prep_usage_data_28_days_flattened") }}
    where metrics_path = 'usage_activity_by_stage_monthly.manage.events'

),
transformed as (

    select
        {{
            dbt_utils.surrogate_key(
                [
                    "first_day_of_month",
                    "self_managed_active_subscriptions.dim_subscription_id",
                ]
            )
        }} as month_subscription_id,
        date_id as dim_date_id,
        self_managed_active_subscriptions.dim_subscription_id,
        mrr * 12 as arr,
        quantity,
        max(prep_usage_ping_payload.dim_subscription_id)
        is not null as has_sent_payloads,
        count(
            distinct prep_usage_ping_payload.dim_usage_ping_id
        ) as monthly_payload_counts,
        count(distinct prep_usage_ping_payload.host_name) as monthly_host_counts,
        max(metric_value) as umau
    from self_managed_active_subscriptions
    inner join
        dim_date on self_managed_active_subscriptions.dim_date_id = dim_date.date_id
    left join
        map_usage_ping_active_subscription
        on self_managed_active_subscriptions.dim_subscription_id
        = map_usage_ping_active_subscription.dim_subscription_id
    left join
        prep_usage_ping_payload
        on map_usage_ping_active_subscription.dim_usage_ping_id
        = prep_usage_ping_payload.dim_usage_ping_id
        and first_day_of_month = prep_usage_ping_payload.ping_created_at_month
    left join
        mau on prep_usage_ping_payload.dim_usage_ping_id = mau.dim_usage_ping_id
        {{ dbt_utils.group_by(n=5) }}

),
latest_versions as (

    select distinct
        date_id as dim_date_id,
        self_managed_active_subscriptions.dim_subscription_id,
        first_value(major_minor_version) OVER (
            partition by
                first_day_of_month,
                self_managed_active_subscriptions.dim_subscription_id
            order by ping_created_at desc
        ) as latest_major_minor_version
    from self_managed_active_subscriptions
    inner join
        dim_date on self_managed_active_subscriptions.dim_date_id = dim_date.date_id
    left join
        map_usage_ping_active_subscription
        on self_managed_active_subscriptions.dim_subscription_id
        = map_usage_ping_active_subscription.dim_subscription_id
    left join
        prep_usage_ping_payload
        on map_usage_ping_active_subscription.dim_usage_ping_id
        = prep_usage_ping_payload.dim_usage_ping_id
        and first_day_of_month = prep_usage_ping_payload.ping_created_at_month

),
joined as (

    select transformed.*, latest_versions.latest_major_minor_version
    from transformed
    left join
        latest_versions
        on transformed.dim_date_id = latest_versions.dim_date_id
        and transformed.dim_subscription_id = latest_versions.dim_subscription_id

)

{{
    dbt_audit(
        cte_ref="joined",
        created_by="@mpeychet_",
        updated_by="@iweeks",
        created_date="2021-06-21",
        updated_date="2022-04-04",
    )
}}
