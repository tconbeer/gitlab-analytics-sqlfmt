{{ config(tags=["mnpi_exception"]) }}

with
    prep_amendment_api_sandbox as (

        select * from {{ ref("prep_amendment_api_sandbox") }}

    ),
    subscription_api_sandbox as (

        select * from {{ ref("prep_subscription_api_sandbox") }}

    ),
    subscription_lineage as (

        select distinct
            subscription_name_slugify,
            subscription_lineage,
            oldest_subscription_in_cohort,
            subscription_cohort_month,
            subscription_cohort_quarter,
            subscription_cohort_year
        from {{ ref("map_subscription_lineage") }}

    ),
    final as (

        select
            -- Surrogate Key
            subscription_api_sandbox.dim_subscription_id,

            -- Natural Key
            subscription_api_sandbox.subscription_name,
            subscription_api_sandbox.subscription_version,

            -- Common Dimension Keys
            subscription_api_sandbox.dim_crm_account_id,
            subscription_api_sandbox.dim_billing_account_id,
            subscription_api_sandbox.dim_billing_account_id_invoice_owner,
            subscription_api_sandbox.dim_crm_opportunity_id,
            {{ get_keyed_nulls("prep_amendment_api_sandbox.dim_amendment_id") }}
            as dim_amendment_id_subscription,

            -- Subscription Information
            subscription_api_sandbox.created_by_id,
            subscription_api_sandbox.updated_by_id,
            subscription_api_sandbox.dim_subscription_id_original,
            subscription_api_sandbox.dim_subscription_id_previous,
            subscription_api_sandbox.subscription_name_slugify,
            subscription_api_sandbox.subscription_status,
            subscription_api_sandbox.zuora_renewal_subscription_name,
            subscription_api_sandbox.zuora_renewal_subscription_name_slugify,
            subscription_api_sandbox.current_term,
            subscription_api_sandbox.renewal_term,
            subscription_api_sandbox.renewal_term_period_type,
            subscription_api_sandbox.eoa_starter_bronze_offer_accepted,
            subscription_api_sandbox.subscription_sales_type,
            subscription_api_sandbox.auto_renew_native_hist,
            subscription_api_sandbox.auto_renew_customerdot_hist,
            subscription_api_sandbox.turn_on_cloud_licensing,
            -- subscription_api_sandbox.turn_on_operational_metrics,
            -- subscription_api_sandbox.contract_operational_metrics,
            subscription_api_sandbox.contract_auto_renewal,
            subscription_api_sandbox.turn_on_auto_renewal,
            subscription_api_sandbox.contract_seat_reconciliation,
            subscription_api_sandbox.turn_on_seat_reconciliation,

            -- Date Information
            subscription_api_sandbox.subscription_start_date,
            subscription_api_sandbox.subscription_end_date,
            subscription_api_sandbox.subscription_start_month,
            subscription_api_sandbox.subscription_end_month,
            subscription_api_sandbox.subscription_end_fiscal_year,
            subscription_api_sandbox.subscription_created_date,
            subscription_api_sandbox.subscription_updated_date,
            subscription_api_sandbox.term_start_date,
            subscription_api_sandbox.term_end_date,
            subscription_api_sandbox.term_start_month,
            subscription_api_sandbox.term_end_month,
            subscription_api_sandbox.second_active_renewal_month,

            -- Lineage and Cohort Information
            subscription_lineage.subscription_lineage,
            subscription_lineage.oldest_subscription_in_cohort,
            subscription_lineage.subscription_cohort_month,
            subscription_lineage.subscription_cohort_quarter,
            subscription_lineage.subscription_cohort_year

        from subscription_api_sandbox
        left join
            subscription_lineage
            on subscription_lineage.subscription_name_slugify
            = subscription_api_sandbox.subscription_name_slugify
        left join
            prep_amendment_api_sandbox
            on subscription_api_sandbox.dim_amendment_id_subscription
            = prep_amendment_api_sandbox.dim_amendment_id

    )

    {{
        dbt_audit(
            cte_ref="final",
            created_by="@ken_aguilar",
            updated_by="@ken_aguilar",
            created_date="2021-08-31",
            updated_date="2021-08-31",
        )
    }}
