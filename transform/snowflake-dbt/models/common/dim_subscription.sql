{{
    config(
        {
            "tags": ["mnpi_exception"],
            "alias": "dim_subscription",
            "post-hook": '{{ apply_dynamic_data_masking(columns = [{"dim_subscription_id":"string"},{"subscription_name":"string"},{"subscription_version":"number"},{"dim_crm_account_id":"string"},{"dim_billing_account_id":"string"},{"dim_billing_account_id_invoice_owner":"string"},{"created_by_id":"string"},{"updated_by_id":"string"},{"dim_subscription_id_original":"string"},{"dim_subscription_id_previous":"string"},{"subscription_name_slugify":"string"},{"subscription_status":"string"},{"namespace_id":"string"},{"namespace_name":"string"},{"zuora_renewal_subscription_name":"string"},{"zuora_renewal_subscription_name_slugify":"array"},{"eoa_starter_bronze_offer_accepted":"string"},     {"subscription_sales_type":"string"},{"auto_renew_customerdot_hist":"string"},{"turn_on_operational_metrics":"string"},{"contract_operational_metrics":"string"},{"contract_auto_renewal":"string"},{"is_questionable_opportunity_mapping":"number"},     {"turn_on_auto_renewal":"string"},{"subscription_start_date":"date"},{"subscription_end_date":"date"},{"subscription_lineage":"string"},{"oldest_subscription_in_cohort":"string"},     {"created_by":"string"},{"updated_by":"string"}          ]) }}',
        }
    )
}}

with
    prep_amendment as (select * from {{ ref("prep_amendment") }}),
    subscription as (select * from {{ ref("prep_subscription") }}),
    subscription_opportunity_mapping as (

        select * from {{ ref("map_subscription_opportunity") }}

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
            subscription.dim_subscription_id,

            -- Natural Key
            subscription.subscription_name,
            subscription.subscription_version,

            -- Common Dimension Keys
            subscription.dim_crm_account_id,
            subscription.dim_billing_account_id,
            subscription.dim_billing_account_id_invoice_owner,
            case
                when subscription.subscription_created_date < '2019-02-01'
                then null
                else subscription_opportunity_mapping.dim_crm_opportunity_id
            end as dim_crm_opportunity_id,
            {{ get_keyed_nulls("prep_amendment.dim_amendment_id") }}
            as dim_amendment_id_subscription,

            -- Subscription Information
            subscription.created_by_id,
            subscription.updated_by_id,
            subscription.dim_subscription_id_original,
            subscription.dim_subscription_id_previous,
            subscription.subscription_name_slugify,
            subscription.subscription_status,
            subscription.namespace_id,
            subscription.namespace_name,
            subscription.zuora_renewal_subscription_name,
            subscription.zuora_renewal_subscription_name_slugify,
            subscription.current_term,
            subscription.renewal_term,
            subscription.renewal_term_period_type,
            subscription.eoa_starter_bronze_offer_accepted,
            subscription.subscription_sales_type,
            subscription.auto_renew_native_hist,
            subscription.auto_renew_customerdot_hist,
            subscription.turn_on_cloud_licensing,
            subscription.turn_on_operational_metrics,
            subscription.contract_operational_metrics,
            subscription.contract_auto_renewal,
            subscription.turn_on_auto_renewal,
            subscription.contract_seat_reconciliation,
            subscription.turn_on_seat_reconciliation,
            subscription_opportunity_mapping.is_questionable_opportunity_mapping,

            -- Date Information
            subscription.subscription_start_date,
            subscription.subscription_end_date,
            subscription.subscription_start_month,
            subscription.subscription_end_month,
            subscription.subscription_end_fiscal_year,
            subscription.subscription_created_date,
            subscription.subscription_updated_date,
            subscription.term_start_date,
            subscription.term_end_date,
            subscription.term_start_month,
            subscription.term_end_month,
            subscription.second_active_renewal_month,

            -- Lineage and Cohort Information
            subscription_lineage.subscription_lineage,
            subscription_lineage.oldest_subscription_in_cohort,
            subscription_lineage.subscription_cohort_month,
            subscription_lineage.subscription_cohort_quarter,
            subscription_lineage.subscription_cohort_year

        from subscription
        left join
            subscription_lineage
            on subscription_lineage.subscription_name_slugify
            = subscription.subscription_name_slugify
        left join
            prep_amendment
            on subscription.dim_amendment_id_subscription
            = prep_amendment.dim_amendment_id
        left join
            subscription_opportunity_mapping
            on subscription.dim_subscription_id
            = subscription_opportunity_mapping.dim_subscription_id

    )

    {{
        dbt_audit(
            cte_ref="final",
            created_by="@snalamaru",
            updated_by="@michellecooper",
            created_date="2020-12-16",
            updated_date="2021-11-11",
        )
    }}
