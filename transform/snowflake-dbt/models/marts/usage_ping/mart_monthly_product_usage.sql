/* grain: one record per host per metric per month */
{{ config(tags=["mnpi_exception"]) }}

{{ config({"materialized": "incremental", "unique_key": "primary_key"}) }}

with
    dim_billing_account as (select * from {{ ref("dim_billing_account") }}),
    dim_crm_accounts as (select * from {{ ref("dim_crm_account") }}),
    dim_date as (

        select distinct first_day_of_month as date_day from {{ ref("dim_date") }}

    ),
    dim_hosts as (select * from {{ ref("dim_hosts") }}),
    dim_instances as (select * from {{ ref("dim_instances") }}),
    dim_license as (select * from {{ ref("dim_license") }}),
    dim_location as (select * from {{ ref("dim_location_country") }}),
    dim_product_detail as (select * from {{ ref("dim_product_detail") }}),
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
    zuora_subscription_snapshots as (

        /**
  This partition handles duplicates and hard deletes by taking only
    the latest subscription version snapshot
   */
        select
            rank() over (
                partition by subscription_name order by dbt_valid_from desc
            ) as rank,
            subscription_id,
            subscription_name
        from {{ ref("zuora_subscription_snapshots_source") }}
        where
            subscription_status not in ('Draft', 'Expired')
            and current_timestamp()::timestamp_tz >= dbt_valid_from
            and {{ coalesce_to_infinity("dbt_valid_to") }}
            > current_timestamp()::timestamp_tz

    ),
    fct_charge as (select * from {{ ref("fct_charge") }}),
    fct_monthly_usage_data as (

        select *
        from {{ ref("monthly_usage_data") }}
        {% if is_incremental() %}

            where created_month >= (select max(reporting_month) from {{ this }})

        {% endif %}

    ),
    dim_usage_pings as (select * from {{ ref("dim_usage_pings") }}),
    subscription_source as (

        select *
        from {{ ref("zuora_subscription_source") }}
        where is_deleted = false and exclude_from_analysis in ('False', '')

    ),
    license_subscriptions as (

        select distinct
            dim_date.date_day as reporting_month,
            dim_license_id as license_id,
            dim_license.license_md5,
            dim_license.company as license_company_name,
            subscription_source.subscription_id as original_linked_subscription_id,
            subscription_source.account_id,
            subscription_source.subscription_name_slugify,
            dim_subscription.dim_subscription_id as latest_active_subscription_id,
            dim_subscription.subscription_start_date,
            dim_subscription.subscription_end_date,
            dim_subscription.subscription_start_month,
            dim_subscription.subscription_end_month,
            dim_billing_account.dim_billing_account_id,
            dim_crm_accounts.crm_account_name,
            dim_crm_accounts.dim_parent_crm_account_id,
            dim_crm_accounts.parent_crm_account_name,
            dim_crm_accounts.parent_crm_account_billing_country,
            dim_crm_accounts.parent_crm_account_sales_segment,
            dim_crm_accounts.parent_crm_account_industry,
            dim_crm_accounts.parent_crm_account_owner_team,
            dim_crm_accounts.parent_crm_account_sales_territory,
            dim_crm_accounts.technical_account_manager,
            iff(max(mrr) > 0, true, false) as is_paid_subscription,
            max(
                iff(product_rate_plan_name ilike any ('%edu%', '%oss%'), true, false)
            ) as is_program_subscription,
            array_agg(distinct dim_product_detail.product_tier_name) within group (
                order by dim_product_detail.product_tier_name asc
            ) as product_category_array,
            array_agg(distinct product_rate_plan_name) within group (
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
        left join
            zuora_subscription_snapshots
            on zuora_subscription_snapshots.subscription_id
            = dim_subscription.dim_subscription_id
            and zuora_subscription_snapshots.rank = 1
        inner join
            fct_charge
            on all_subscriptions.subscription_id = fct_charge.dim_subscription_id
            and charge_type = 'Recurring'
        inner join
            dim_product_detail
            on dim_product_detail.dim_product_detail_id
            = fct_charge.dim_product_detail_id
            and dim_product_detail.product_delivery_type = 'Self-Managed'
            and product_rate_plan_name not in ('Premium - 1 Year - Eval')
        left join
            dim_billing_account
            on dim_subscription.dim_billing_account_id
            = dim_billing_account.dim_billing_account_id
        left join
            dim_crm_accounts
            on dim_billing_account.dim_crm_account_id
            = dim_crm_accounts.dim_crm_account_id
        inner join
            dim_date
            on effective_start_month <= dim_date.date_day
            and effective_end_month > dim_date.date_day
            {{ dbt_utils.group_by(n=22) }}

    ),
    joined as (

        select
            fct_monthly_usage_data.ping_id,
            fct_monthly_usage_data.created_month,
            fct_monthly_usage_data.metrics_path,
            fct_monthly_usage_data.group_name,
            fct_monthly_usage_data.stage_name,
            fct_monthly_usage_data.section_name,
            fct_monthly_usage_data.is_smau,
            fct_monthly_usage_data.is_gmau,
            fct_monthly_usage_data.is_paid_gmau,
            fct_monthly_usage_data.is_umau,
            dim_usage_pings.license_md5,
            dim_usage_pings.license_trial_ends_on,
            dim_usage_pings.is_trial,
            dim_usage_pings.umau_value,
            license_subscriptions.license_id,
            license_subscriptions.license_company_name,
            license_subscriptions.original_linked_subscription_id,
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
            dim_usage_pings.ping_source as delivery,
            dim_usage_pings.main_edition as main_edition,
            dim_usage_pings.edition,
            dim_usage_pings.product_tier as ping_product_tier,
            dim_usage_pings.main_edition_product_tier as ping_main_edition_product_tier,
            dim_usage_pings.major_version,
            dim_usage_pings.minor_version,
            dim_usage_pings.major_minor_version,
            dim_usage_pings.version,
            dim_usage_pings.is_pre_release,
            dim_usage_pings.is_internal,
            dim_usage_pings.is_staging,
            dim_usage_pings.instance_user_count,
            dim_usage_pings.created_at,
            dim_usage_pings.recorded_at,
            time_period,
            monthly_metric_value,
            original_metric_value,
            dim_hosts.host_id,
            dim_hosts.source_ip_hash,
            dim_usage_pings.uuid as instance_id,
            dim_usage_pings.hostname as host_name,
            dim_hosts.location_id,
            dim_location.country_name,
            dim_location.iso_2_country_code
        from fct_monthly_usage_data
        left join dim_usage_pings on fct_monthly_usage_data.ping_id = dim_usage_pings.id
        left join
            dim_hosts
            on dim_usage_pings.host_id = dim_hosts.host_id
            and dim_usage_pings.source_ip_hash = dim_hosts.source_ip_hash
            and dim_usage_pings.uuid = dim_hosts.instance_id
        left join
            license_subscriptions
            on dim_usage_pings.license_md5 = license_subscriptions.license_md5
            and fct_monthly_usage_data.created_month
            = license_subscriptions.reporting_month
        left join
            dim_location on dim_hosts.location_id = dim_location.dim_location_country_id

    ),
    sorted as (

        select

            -- Primary Key
            {{
                dbt_utils.surrogate_key(
                    [
                        "metrics_path",
                        "created_month",
                        "instance_id",
                        "host_id",
                        "host_name",
                    ]
                )
            }} as primary_key,
            created_month as reporting_month,
            metrics_path,
            ping_id,

            -- Foreign Key
            host_id,
            instance_id,
            license_id,
            license_md5,
            original_linked_subscription_id,
            latest_active_subscription_id,
            dim_billing_account_id,
            location_id,
            dim_parent_crm_account_id,

            -- metadata usage ping
            delivery,
            main_edition,
            edition,
            ping_product_tier,
            ping_main_edition_product_tier,
            major_version,
            minor_version,
            major_minor_version,
            version,
            is_pre_release,
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

            -- metatadata hosts
            source_ip_hash,
            host_name,

            -- metadata instance
            instance_user_count,

            -- metadata subscription
            license_company_name,
            subscription_name_slugify,
            subscription_start_month,
            subscription_end_month,
            product_category_array,
            product_rate_plan_name_array,
            is_paid_subscription,
            is_program_subscription,
            license_trial_ends_on,

            -- account metadata
            crm_account_name,
            parent_crm_account_name,
            parent_crm_account_billing_country,
            parent_crm_account_sales_segment,
            parent_crm_account_industry,
            parent_crm_account_owner_team,
            parent_crm_account_sales_territory,
            technical_account_manager,

            -- location info
            country_name as ping_country_name,
            iso_2_country_code as ping_country_code,

            created_at,
            recorded_at,

            -- monthly_usage_data
            time_period,
            monthly_metric_value,
            original_metric_value

        from joined

    )

    {{
        dbt_audit(
            cte_ref="sorted",
            created_by="@mpeychet",
            updated_by="@mcooperDD",
            created_date="2020-12-01",
            updated_date="2020-03-05",
        )
    }}
