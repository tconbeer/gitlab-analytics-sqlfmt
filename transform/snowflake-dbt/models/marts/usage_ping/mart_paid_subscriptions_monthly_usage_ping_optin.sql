{{ config(tags=["mnpi_exception"]) }}

-- PENDING SCHEMA MIGRATION
with
    fct_mrr as (

        select *
        from {{ ref("fct_mrr") }}
        where subscription_status in ('Active', 'Cancelled')

    ),
    dim_product_detail as (select * from {{ ref("dim_product_detail") }}),
    self_managed_active_subscriptions as (

        select
            dim_date_id as date_id,
            dim_subscription_id as subscription_id,
            sum(mrr) as mrr,
            sum(quantity) as quantity
        from fct_mrr
        inner join
            dim_product_detail
            on fct_mrr.dim_product_detail_id = dim_product_detail.dim_product_detail_id
            and product_delivery_type = 'Self-Managed'
            {{ dbt_utils.group_by(n=2) }}

    ),
    dim_date as (

        select distinct date_id, first_day_of_month
        from {{ ref("dim_date") }}
        where first_day_of_month <= current_date

    ),
    active_subscriptions as (

        select *
        from {{ ref("dim_subscription") }}
        where subscription_status not in ('Draft', 'Expired')

    ),
    all_subscriptions as (select * from {{ ref("dim_subscription") }}),
    fct_payload as (select * from {{ ref("fct_usage_ping_payload") }}),
    prep_license as (select * from {{ ref("prep_license") }}),
    mau as (

        select *
        from {{ ref("poc_prep_usage_data_28_days_flattened") }}
        where metrics_path = 'usage_activity_by_stage_monthly.manage.events'

    ),
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
            quantity,
            max(fct_payload.dim_subscription_id) is not null as has_sent_payloads,
            count(distinct fct_payload.dim_usage_ping_id) as monthly_payload_counts,
            count(distinct fct_payload.host_name) as monthly_host_counts,
            max(license_user_count) as license_user_count,
            max(metric_value) as umau
        from self_managed_active_subscriptions
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
            and first_day_of_month = date_trunc('month', fct_payload.ping_created_at)
        left join mau on fct_payload.dim_usage_ping_id = mau.ping_id
        left join
            prep_license on fct_payload.dim_license_id = prep_license.dim_license_id
            {{ dbt_utils.group_by(n=7) }}

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
    joined as (

        select transformed.*, latest_versions.latest_major_minor_version
        from transformed
        left join
            latest_versions
            on transformed.reporting_month = latest_versions.reporting_month
            and transformed.subscription_name_slugify
            = latest_versions.subscription_name_slugify

    )

    {{
        dbt_audit(
            cte_ref="joined",
            created_by="@mpeychet_",
            updated_by="@jpeguero",
            created_date="2020-10-16",
            updated_date="2022-02-17",
        )
    }}
