{{ config(tags=["mnpi_exception"]) }}

{{ config({"materialized": "table", "schema": "common_mart_product"}) }}

{{
    simple_cte(
        [
            ("dim_billing_account", "dim_billing_account"),
            ("dim_crm_account", "dim_crm_account"),
            ("dim_date", "dim_date"),
            ("dim_product_detail", "dim_product_detail"),
            ("fct_usage_ping_payload", "fct_usage_ping_payload"),
            ("dim_subscription", "dim_subscription"),
            ("dim_license", "dim_license"),
            ("dim_location", "dim_location_country"),
        ]
    )
}},
joined as (

    select
        fct_usage_ping_payload.*,
        dim_billing_account.dim_billing_account_id,
        dim_crm_account.dim_crm_account_id,
        dim_crm_account.crm_account_name,
        dim_crm_account.crm_account_billing_country,
        dim_crm_account.dim_parent_crm_account_id,
        dim_crm_account.parent_crm_account_sales_segment,
        dim_crm_account.parent_crm_account_billing_country,
        dim_crm_account.parent_crm_account_industry,
        dim_crm_account.parent_crm_account_owner_team,
        dim_crm_account.parent_crm_account_sales_territory,
        dim_date.date_actual,
        dim_date.first_day_of_month,
        dim_date.fiscal_quarter_name_fy,
        dim_location.country_name,
        dim_location.iso_2_country_code,
        dim_license.license_md5
    from fct_usage_ping_payload
    left join
        dim_subscription
        on fct_usage_ping_payload.dim_subscription_id
        = dim_subscription.dim_subscription_id
    left join
        dim_billing_account
        on dim_subscription.dim_billing_account_id
        = dim_billing_account.dim_billing_account_id
    left join
        dim_crm_account
        on dim_billing_account.dim_crm_account_id = dim_crm_account.dim_crm_account_id
    left join dim_date on fct_usage_ping_payload.dim_date_id = dim_date.date_id
    left join
        dim_location
        on fct_usage_ping_payload.dim_location_country_id
        = dim_location.dim_location_country_id
    left join
        dim_license
        on fct_usage_ping_payload.dim_license_id = dim_license.dim_license_id

),
renamed as (

    select
        -- keys
        dim_usage_ping_id,
        host_name,
        dim_instance_id,

        -- date info
        dim_date_id,
        ping_created_at,
        ping_created_at_date,
        ping_created_at_month,
        fiscal_quarter_name_fy as ping_fiscal_quarter,
        iff(
            row_number() over (
                partition by dim_instance_id, ping_created_at_month
                order by dim_usage_ping_id desc
            )
            = 1,
            true,
            false
        ) as is_last_ping_in_month,
        iff(
            row_number() over (
                partition by dim_instance_id, fiscal_quarter_name_fy
                order by dim_usage_ping_id desc
            )
            = 1,
            true,
            false
        ) as is_last_ping_in_quarter,

        -- customer info
        dim_billing_account_id,
        dim_crm_account_id,
        crm_account_name,
        crm_account_billing_country,
        dim_parent_crm_account_id,
        parent_crm_account_billing_country,
        parent_crm_account_industry,
        parent_crm_account_owner_team,
        parent_crm_account_sales_segment,
        parent_crm_account_sales_territory,

        -- product info
        license_md5,
        -- is_trial                AS ping_is_trial_license,
        -- might rename it in the payload model
        product_tier as ping_product_tier,

        -- location info
        dim_location_country_id,
        country_name as ping_country_name,
        iso_2_country_code as ping_country_code,

        -- metadata
        usage_ping_delivery_type,
        edition,
        major_minor_version,
        version_is_prerelease,
        instance_user_count
    from joined
    where host_name not in ('staging.gitlab.com', 'dr.gitlab.com')

)

select *
from renamed
