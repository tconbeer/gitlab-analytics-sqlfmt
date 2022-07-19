{{
    config(
        tags=["product", "mnpi_exception"],
        materialized="incremental",
        unique_key="mart_service_ping_instance_metric_id",
    )
}}

{{
    simple_cte(
        [
            ("fct_service_ping_instance_metric", "fct_service_ping_instance_metric"),
            ("dim_service_ping", "dim_service_ping_instance"),
            ("dim_product_tier", "dim_product_tier"),
            ("dim_date", "dim_date"),
            ("dim_billing_account", "dim_billing_account"),
            ("dim_crm_accounts", "dim_crm_account"),
            ("dim_product_detail", "dim_product_detail"),
            ("fct_charge", "fct_charge"),
            ("dim_license", "dim_license"),
            ("dim_hosts", "dim_hosts"),
            ("dim_location", "dim_location_country"),
            ("dim_service_ping_metric", "dim_service_ping_metric"),
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
fct_service_ping as (

    select *
    from fct_service_ping_instance_metric
    where
        is_real(to_variant(metric_value))
        {% if is_incremental() %}
        and ping_created_at >= (select max(ping_created_at) from {{ this }})
        {% endif %}

),
subscription_source as (

    select *
    from {{ ref("zuora_subscription_source") }}
    where is_deleted = false and exclude_from_analysis in ('False', '')

),
license_subscriptions as (

    select distinct
        dim_date.first_day_of_month as reporting_month,
        dim_license_id as license_id,
        dim_license.license_md5 as license_md5,
        dim_license.company as license_company_name,
        subscription_source.subscription_name_slugify
        as original_subscription_name_slugify,
        dim_subscription.dim_subscription_id as latest_active_subscription_id,
        dim_subscription.subscription_start_date as subscription_start_date,
        dim_subscription.subscription_end_date as subscription_end_date,
        dim_subscription.subscription_start_month as subscription_start_month,
        dim_subscription.subscription_end_month as subscription_end_month,
        dim_billing_account.dim_billing_account_id as dim_billing_account_id,
        dim_crm_accounts.crm_account_name as crm_account_name,
        dim_crm_accounts.dim_parent_crm_account_id as dim_parent_crm_account_id,
        dim_crm_accounts.parent_crm_account_name as parent_crm_account_name,
        dim_crm_accounts.parent_crm_account_billing_country
        as parent_crm_account_billing_country,
        dim_crm_accounts.parent_crm_account_sales_segment
        as parent_crm_account_sales_segment,
        dim_crm_accounts.parent_crm_account_industry as parent_crm_account_industry,
        dim_crm_accounts.parent_crm_account_owner_team as parent_crm_account_owner_team,
        dim_crm_accounts.parent_crm_account_sales_territory
        as parent_crm_account_sales_territory,
        dim_crm_accounts.technical_account_manager as technical_account_manager,
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
    from dim_license
    inner join
        subscription_source
        on dim_license.dim_subscription_id = subscription_source.subscription_id
    left join
        dim_subscription
        on subscription_source.subscription_name_slugify
        = dim_subscription.subscription_name_slugify
    left join
        subscription_source as all_subscriptions
        on subscription_source.subscription_name_slugify
        = all_subscriptions.subscription_name_slugify
    inner join
        fct_charge
        on all_subscriptions.subscription_id = fct_charge.dim_subscription_id
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
        dim_crm_accounts
        on dim_billing_account.dim_crm_account_id = dim_crm_accounts.dim_crm_account_id
    inner join
        dim_date
        on effective_start_month <= dim_date.date_day
        and effective_end_month > dim_date.date_day
        {{ dbt_utils.group_by(n=20) }}


),
joined as (

    select
        fct_service_ping.dim_service_ping_date_id as dim_service_ping_date_id,
        fct_service_ping.dim_license_id as dim_license_id,
        fct_service_ping.dim_installation_id as dim_installation_id,
        fct_service_ping.dim_service_ping_instance_id as dim_service_ping_instance_id,
        fct_service_ping.metrics_path as metrics_path,
        fct_service_ping.metric_value as metric_value,
        fct_service_ping.has_timed_out as has_timed_out,
        dim_service_ping_metric.group_name as group_name,
        dim_service_ping_metric.stage_name as stage_name,
        dim_service_ping_metric.section_name as section_name,
        dim_service_ping_metric.is_smau as is_smau,
        dim_service_ping_metric.is_gmau as is_gmau,
        dim_service_ping_metric.is_paid_gmau as is_paid_gmau,
        dim_service_ping_metric.is_umau as is_umau,
        dim_service_ping.license_md5 as license_md5,
        dim_service_ping.is_trial as is_trial,
        fct_service_ping.umau_value as umau_value,
        license_subscriptions.license_id as license_id,
        license_subscriptions.license_company_name as license_company_name,
        license_subscriptions.latest_active_subscription_id
        as latest_active_subscription_id,
        license_subscriptions.original_subscription_name_slugify
        as original_subscription_name_slugify,
        license_subscriptions.product_category_array as product_category_array,
        license_subscriptions.product_rate_plan_name_array
        as product_rate_plan_name_array,
        license_subscriptions.subscription_start_month as subscription_start_month,
        license_subscriptions.subscription_end_month as subscription_end_month,
        license_subscriptions.dim_billing_account_id as dim_billing_account_id,
        license_subscriptions.crm_account_name as crm_account_name,
        license_subscriptions.dim_parent_crm_account_id as dim_parent_crm_account_id,
        license_subscriptions.parent_crm_account_name as parent_crm_account_name,
        license_subscriptions.parent_crm_account_billing_country
        as parent_crm_account_billing_country,
        license_subscriptions.parent_crm_account_sales_segment
        as parent_crm_account_sales_segment,
        license_subscriptions.parent_crm_account_industry
        as parent_crm_account_industry,
        license_subscriptions.parent_crm_account_owner_team
        as parent_crm_account_owner_team,
        license_subscriptions.parent_crm_account_sales_territory
        as parent_crm_account_sales_territory,
        license_subscriptions.technical_account_manager as technical_account_manager,
        coalesce(is_paid_subscription, false) as is_paid_subscription,
        coalesce(is_program_subscription, false) as is_program_subscription,
        dim_service_ping.service_ping_delivery_type as service_ping_delivery_type,
        dim_service_ping.ping_edition as ping_edition,
        dim_service_ping.product_tier as ping_product_tier,
        dim_service_ping.ping_edition
        || ' - '
        || dim_service_ping.product_tier as ping_edition_product_tier,
        dim_service_ping.major_version as major_version,
        dim_service_ping.minor_version as minor_version,
        dim_service_ping.major_minor_version as major_minor_version,
        dim_service_ping.major_minor_version_id as major_minor_version_id,
        dim_service_ping.version_is_prerelease as version_is_prerelease,
        dim_service_ping.is_internal as is_internal,
        dim_service_ping.is_staging as is_staging,
        dim_service_ping.instance_user_count as instance_user_count,
        dim_service_ping.ping_created_at as ping_created_at,
        dim_date.first_day_of_month as ping_created_at_month,
        fct_service_ping.time_frame as time_frame,
        fct_service_ping.dim_host_id as dim_host_id,
        fct_service_ping.dim_instance_id as dim_instance_id,
        dim_service_ping.host_name as host_name,
        dim_service_ping.is_last_ping_of_month as is_last_ping_of_month,
        fct_service_ping.dim_location_country_id as dim_location_country_id,
        dim_location.country_name as country_name,
        dim_location.iso_2_country_code as iso_2_country_code
    from fct_service_ping
    left join
        dim_service_ping_metric
        on fct_service_ping.metrics_path = dim_service_ping_metric.metrics_path
    inner join dim_date on fct_service_ping.dim_service_ping_date_id = dim_date.date_id
    left join
        dim_service_ping
        on fct_service_ping.dim_service_ping_instance_id
        = dim_service_ping.dim_service_ping_instance_id
    left join
        dim_hosts
        on dim_service_ping.dim_host_id = dim_hosts.host_id
        and dim_service_ping.ip_address_hash = dim_hosts.source_ip_hash
        and dim_service_ping.dim_instance_id = dim_hosts.instance_id
    left join
        license_subscriptions
        on dim_service_ping.license_md5 = license_subscriptions.license_md5
        and dim_date.first_day_of_month = license_subscriptions.reporting_month
    left join
        dim_location
        on fct_service_ping.dim_location_country_id
        = dim_location.dim_location_country_id

),
sorted as (

    select

        -- Primary Key
        {{ dbt_utils.surrogate_key(["dim_service_ping_instance_id", "metrics_path"]) }}
        as mart_service_ping_instance_metric_id,
        dim_service_ping_date_id,
        metrics_path,
        metric_value,
        has_timed_out,
        dim_service_ping_instance_id,

        -- Foreign Key
        dim_instance_id,
        dim_license_id,
        dim_installation_id,
        latest_active_subscription_id,
        dim_billing_account_id,
        dim_parent_crm_account_id,
        major_minor_version_id,
        dim_host_id,
        host_name,
        -- metadata usage ping
        service_ping_delivery_type,
        ping_edition,
        ping_product_tier,
        ping_edition_product_tier,
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
        time_frame,

        -- metadata instance
        instance_user_count,

        -- metadata subscription
        original_subscription_name_slugify,
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
        ping_created_at_month,
        is_last_ping_of_month


    from joined
    where time_frame != 'none' and try_to_decimal(metric_value::text) >= 0

)

{{
    dbt_audit(
        cte_ref="sorted",
        created_by="@icooper-acp",
        updated_by="@icooper-acp",
        created_date="2022-03-11",
        updated_date="2022-03-11",
    )
}}
