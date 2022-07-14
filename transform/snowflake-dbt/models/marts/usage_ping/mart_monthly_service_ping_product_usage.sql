-- grain: one record per host per metric per month
-- curently missing host_id 
{{ config(tags=["mnpi_exception"]) }}

{{ config({"materialized": "table", "schema": "common_mart_product"}) }}

{{
    simple_cte(
        [
            ("dim_billing_account", "dim_billing_account"),
            ("dim_crm_account", "dim_crm_account"),
            ("dim_date", "dim_date"),
            ("dim_instances", "dim_instances"),
            ("dim_product_detail", "dim_product_detail"),
        ]
    )
}},
dim_subscription as (

    select *
    from {{ ref("dim_subscription") }}
    where
        (
            subscription_name_slugify
            <> zuora_renewal_subscription_name_slugify[0]::text
            or zuora_renewal_subscription_name_slugify is null
        )
        and subscription_status not in ('Draft', 'Expired')

),
fct_charge as (select * from {{ ref("fct_charge") }}),
fct_monthly_usage_data as (

    select *
    from {{ ref("fct_monthly_usage_data") }}
    {% if is_incremental() %}

    where ping_created_month >= (select max(reporting_month) from {{ this }})

    {% endif %}

),
fct_usage_ping_payload as (select * from {{ ref("fct_usage_ping_payload") }}),
subscription_source as (

    select *
    from {{ ref("zuora_subscription_source") }}
    where is_deleted = false and exclude_from_analysis in ('False', '')

),
license_subscriptions as (

    select distinct
        dim_date.date_day as reporting_month,
        dim_subscription.subscription_name_slugify,
        dim_subscription.dim_subscription_id as latest_active_subscription_id,
        dim_subscription.subscription_start_date,
        dim_subscription.subscription_end_date,
        dim_subscription.subscription_start_month,
        dim_subscription.subscription_end_month,
        dim_billing_account.dim_billing_account_id,
        dim_crm_account.crm_account_name,
        dim_crm_account.dim_parent_crm_account_id,
        dim_crm_account.parent_crm_account_name,
        dim_crm_account.parent_crm_account_billing_country,
        dim_crm_account.parent_crm_account_sales_segment,
        dim_crm_account.parent_crm_account_industry,
        dim_crm_account.parent_crm_account_owner_team,
        dim_crm_account.parent_crm_account_sales_territory,
        dim_crm_account.technical_account_manager,
        iff(max(mrr) > 0, true, false) as is_paid_subscription,
        max(
            iff(product_rate_plan_name ilike any ('%edu%', '%oss%'), true, false)
        ) as is_program_subscription,
        array_agg(distinct dim_product_detail.product_tier_name)
        within group(
            order by dim_product_detail.product_tier_name asc
        ) as product_category_array,
        array_agg(distinct product_rate_plan_name)
        within group(
            order by product_rate_plan_name asc
        ) as product_rate_plan_name_array,
        sum(quantity) as quantity,
        sum(mrr * 12) as arr
    from dim_subscription
    inner join
        fct_charge
        on dim_subscription.dim_subscription_id = fct_charge.dim_subscription_id
        and charge_type = 'Recurring'
    inner join
        dim_product_detail
        on dim_product_detail.dim_product_detail_id = fct_charge.dim_product_detail_id
        and dim_product_detail.product_delivery_type = 'Self-Managed'
        and product_rate_plan_name not in ('Premium - 1 Year - Eval')
    left join
        dim_billing_account
        on dim_subscription.dim_billing_account_id
        = dim_billing_account.dim_billing_account_id
    left join
        dim_crm_account
        on dim_billing_account.dim_crm_account_id = dim_crm_account.dim_crm_account_id
    inner join
        dim_date
        on effective_start_month <= dim_date.date_day
        and effective_end_month > dim_date.date_day
        and dim_date.date_day = dim_date.first_day_of_month
        {{ dbt_utils.group_by(n=17) }}

),
joined as (

    select
        fct_usage_ping_payload.dim_usage_ping_id,
        fct_monthly_usage_data.ping_created_month,
        fct_monthly_usage_data.metrics_path,
        fct_monthly_usage_data.group_name,
        fct_monthly_usage_data.stage_name,
        fct_monthly_usage_data.section_name,
        fct_monthly_usage_data.is_smau,
        fct_monthly_usage_data.is_gmau,
        fct_monthly_usage_data.is_paid_gmau,
        fct_monthly_usage_data.is_umau,
        fct_usage_ping_payload.dim_license_id,
        fct_usage_ping_payload.is_trial,
        fct_usage_ping_payload.umau_value,
        license_subscriptions.latest_active_subscription_id,
        license_subscriptions.subscription_name_slugify,
        license_subscriptions.product_category_array,
        license_subscriptions.product_rate_plan_name_array,
        license_subscriptions.subscription_start_month,
        license_subscriptions.subscription_end_month,
        license_subscriptions.dim_billing_account_id,
        license_subscriptions.crm_account_name,
        license_subscriptions.dim_parent_crm_account_id,
        license_subscriptions.parent_crm_account_name,
        license_subscriptions.parent_crm_account_billing_country,
        license_subscriptions.parent_crm_account_sales_segment,
        license_subscriptions.parent_crm_account_industry,
        license_subscriptions.parent_crm_account_owner_team,
        license_subscriptions.parent_crm_account_sales_territory,
        license_subscriptions.technical_account_manager,
        coalesce(is_paid_subscription, false) as is_paid_subscription,
        coalesce(is_program_subscription, false) as is_program_subscription,
        fct_usage_ping_payload.usage_ping_delivery_type,
        fct_usage_ping_payload.edition,
        fct_usage_ping_payload.product_tier as ping_product_tier,
        fct_usage_ping_payload.edition_product_tier as ping_main_edition_product_tier,
        fct_usage_ping_payload.major_version,
        fct_usage_ping_payload.minor_version,
        fct_usage_ping_payload.major_minor_version,
        fct_usage_ping_payload.version_is_prerelease,
        fct_usage_ping_payload.is_internal,
        fct_usage_ping_payload.is_staging,
        fct_usage_ping_payload.instance_user_count,
        fct_usage_ping_payload.ping_created_at,
        time_period,
        monthly_metric_value,
        original_metric_value,
        fct_usage_ping_payload.dim_instance_id,
        fct_usage_ping_payload.host_name
    from fct_monthly_usage_data
    left join
        fct_usage_ping_payload
        on fct_monthly_usage_data.dim_usage_ping_id
        = fct_usage_ping_payload.dim_usage_ping_id
    left join
        {{ ref("map_usage_ping_active_subscription") }} act_sub
        on fct_usage_ping_payload.dim_usage_ping_id = act_sub.dim_usage_ping_id
    left join
        license_subscriptions
        on act_sub.dim_subscription_id
        = license_subscriptions.latest_active_subscription_id
        and ping_created_month = reporting_month

),
sorted as (

    select

        -- Primary Key
        {{
            dbt_utils.surrogate_key(
                ["metrics_path", "ping_created_month", "dim_instance_id", "host_name"]
            )
        }} as primary_key,
        ping_created_month as reporting_month,
        metrics_path,
        dim_usage_ping_id,

        -- Foreign Key
        dim_instance_id,
        dim_license_id,
        latest_active_subscription_id,
        dim_billing_account_id,
        dim_parent_crm_account_id,
        host_name,
        -- metadata usage ping
        usage_ping_delivery_type,
        edition,
        ping_product_tier,
        ping_main_edition_product_tier,
        major_version,
        minor_version,
        major_minor_version,
        version_is_prerelease,
        is_internal,
        is_staging,
        is_trial,
        umau_value,

        -- metadata metrics
        group_name,
        stage_name,
        section_name,
        is_smau,
        is_gmau,
        is_paid_gmau,
        is_umau,

        -- metadata instance
        instance_user_count,

        -- metadata subscription
        subscription_name_slugify,
        subscription_start_month,
        subscription_end_month,
        product_category_array,
        product_rate_plan_name_array,
        is_paid_subscription,
        is_program_subscription,

        -- account metadata
        crm_account_name,
        parent_crm_account_name,
        parent_crm_account_billing_country,
        parent_crm_account_sales_segment,
        parent_crm_account_industry,
        parent_crm_account_owner_team,
        parent_crm_account_sales_territory,
        technical_account_manager,

        ping_created_at,

        -- fct_monthly_usage_data
        time_period,
        monthly_metric_value,
        original_metric_value

    from joined

)

{{
    dbt_audit(
        cte_ref="sorted",
        created_by="@mpeychet",
        updated_by="@chrissharp",
        created_date="2021-06-17",
        updated_date="2022-01-20",
    )
}}
