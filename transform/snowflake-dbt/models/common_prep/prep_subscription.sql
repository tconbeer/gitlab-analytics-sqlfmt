{{ config(tags=["mnpi_exception"]) }}

with
    date_details as (select * from {{ ref("date_details") }}),
    map_merged_crm_account as (select * from {{ ref("map_merged_crm_account") }}),
    zuora_subscription as (

        select *
        from {{ ref("zuora_subscription_source") }}
        where is_deleted = false and exclude_from_analysis in ('False', '')

    ),
    zuora_account as (select account_id, crm_id from {{ ref("zuora_account_source") }}),
    joined as (

        select
            zuora_subscription.subscription_id as dim_subscription_id,
            map_merged_crm_account.dim_crm_account_id as dim_crm_account_id,
            zuora_account.account_id as dim_billing_account_id,
            zuora_subscription.invoice_owner_id as dim_billing_account_id_invoice_owner,
            zuora_subscription.sfdc_opportunity_id as dim_crm_opportunity_id,
            zuora_subscription.original_id as dim_subscription_id_original,
            zuora_subscription.previous_subscription_id as dim_subscription_id_previous,
            zuora_subscription.amendment_id as dim_amendment_id_subscription,
            zuora_subscription.created_by_id,
            zuora_subscription.updated_by_id,
            zuora_subscription.subscription_name,
            zuora_subscription.subscription_name_slugify,
            zuora_subscription.subscription_status,
            zuora_subscription.version as subscription_version,
            zuora_subscription.zuora_renewal_subscription_name,
            zuora_subscription.zuora_renewal_subscription_name_slugify,
            zuora_subscription.current_term,
            zuora_subscription.renewal_term,
            zuora_subscription.renewal_term_period_type,
            zuora_subscription.eoa_starter_bronze_offer_accepted,
            -- All Self-Service / Web direct subscriptions are identified by these
            -- created_by_ids
            iff(
                zuora_subscription.created_by_id in (
                    '2c92a0107bde3653017bf00cd8a86d5a',
                    '2c92a0fd55822b4d015593ac264767f2'
                ),
                'Self-Service',
                'Sales-Assisted'
            ) as subscription_sales_type,
            zuora_subscription.namespace_name,
            zuora_subscription.namespace_id,

            -- Date Information
            zuora_subscription.subscription_start_date as subscription_start_date,
            zuora_subscription.subscription_end_date as subscription_end_date,
            date_trunc(
                'month', zuora_subscription.subscription_start_date::date
            ) as subscription_start_month,
            date_trunc(
                'month', zuora_subscription.subscription_end_date::date
            ) as subscription_end_month,
            date_details.fiscal_year as subscription_end_fiscal_year,
            date_details.fiscal_quarter_name_fy
            as subscription_end_fiscal_quarter_name_fy,
            zuora_subscription.term_start_date::date as term_start_date,
            zuora_subscription.term_end_date::date as term_end_date,
            date_trunc(
                'month', zuora_subscription.term_start_date::date
            ) as term_start_month,
            date_trunc(
                'month', zuora_subscription.term_end_date::date
            ) as term_end_month,
            case
                when
                    lower(
                        zuora_subscription.subscription_status
                    ) = 'active' and subscription_end_date > current_date
                then
                    date_trunc(
                        'month',
                        dateadd(
                            'month',
                            zuora_subscription.current_term,
                            zuora_subscription.subscription_end_date::date
                        )
                    )
                else null
            end as second_active_renewal_month,
            zuora_subscription.auto_renew_native_hist,
            zuora_subscription.auto_renew_customerdot_hist,
            zuora_subscription.turn_on_cloud_licensing,
            zuora_subscription.turn_on_operational_metrics,
            zuora_subscription.contract_operational_metrics,
            -- zuora_subscription.turn_on_usage_ping_required_metrics,
            -- https://gitlab.com/gitlab-data/analytics/-/issues/10172
            null as turn_on_usage_ping_required_metrics,
            zuora_subscription.contract_auto_renewal,
            zuora_subscription.turn_on_auto_renewal,
            zuora_subscription.contract_seat_reconciliation,
            zuora_subscription.turn_on_seat_reconciliation,
            zuora_subscription.created_date::date as subscription_created_date,
            zuora_subscription.updated_date::date as subscription_updated_date
        from zuora_subscription
        inner join
            zuora_account on zuora_subscription.account_id = zuora_account.account_id
        left join
            map_merged_crm_account
            on zuora_account.crm_id = map_merged_crm_account.sfdc_account_id
        left join
            date_details
            on zuora_subscription.subscription_end_date::date = date_details.date_day

    )

    {{
        dbt_audit(
            cte_ref="joined",
            created_by="@ischweickartDD",
            updated_by="@michellecooper",
            created_date="2021-01-07",
            updated_date="2022-01-19",
        )
    }}
