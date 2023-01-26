{{ config({"tags": ["arr_snapshots", "mnpi_exception"], "schema": "common"}) }}

with
    snapshot_dates as (

        select *
        from {{ ref("dim_date") }}
        where date_actual >= '2020-03-01' and date_actual <= current_date

    ),
    zuora_account as (

        select *
        from {{ ref("zuora_account_snapshots_source") }}
        where is_deleted = false and lower(live_batch) != 'batch20'

    ),
    zuora_account_spined as (

        select snapshot_dates.date_id as snapshot_id, zuora_account.*
        from zuora_account
        inner join
            snapshot_dates
            on snapshot_dates.date_actual >= zuora_account.dbt_valid_from
            and snapshot_dates.date_actual
            < {{ coalesce_to_infinity("zuora_account.dbt_valid_to") }}

    ),
    zuora_subscription as (

        select *
        from {{ ref("zuora_subscription_snapshots_source") }}
        where
            lower(subscription_status) not in ('draft', 'expired')
            and is_deleted = false
            and exclude_from_analysis in ('False', '')

    ),
    zuora_subscription_spined as (

        select snapshot_dates.date_id as snapshot_id, zuora_subscription.*
        from zuora_subscription
        inner join
            snapshot_dates
            on snapshot_dates.date_actual >= zuora_subscription.dbt_valid_from
            and snapshot_dates.date_actual
            < {{ coalesce_to_infinity("zuora_subscription.dbt_valid_to") }}
        qualify
            rank() over (
                partition by subscription_name, snapshot_dates.date_actual
                order by dbt_valid_from desc
            )
            = 1

    ),
    map_merged_crm_account as (select * from {{ ref("map_merged_crm_account") }}),
    prep_amendment as (select * from {{ ref("prep_amendment") }}),
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
    joined as (

        select
            -- Surrogate Key
            zuora_subscription_spined.snapshot_id as snapshot_id,
            zuora_subscription_spined.subscription_id as dim_subscription_id,

            -- Natural Key
            zuora_subscription_spined.subscription_name as subscription_name,
            zuora_subscription_spined.version as subscription_version,

            -- Common Dimension Keys
            map_merged_crm_account.dim_crm_account_id as dim_crm_account_id,
            zuora_account_spined.account_id as dim_billing_account_id,
            zuora_subscription_spined.invoice_owner_id
            as dim_billing_account_id_invoice_owner,
            zuora_subscription_spined.sfdc_opportunity_id as dim_crm_opportunity_id,
            {{ get_keyed_nulls("prep_amendment.dim_amendment_id") }}
            as dim_amendment_id_subscription,

            -- Subscription Information
            zuora_subscription_spined.created_by_id,
            zuora_subscription_spined.updated_by_id,
            zuora_subscription_spined.original_id as dim_subscription_id_original,
            zuora_subscription_spined.previous_subscription_id
            as dim_subscription_id_previous,
            zuora_subscription_spined.subscription_name_slugify,
            zuora_subscription_spined.subscription_status,
            zuora_subscription_spined.auto_renew_native_hist,
            zuora_subscription_spined.auto_renew_customerdot_hist,
            zuora_subscription_spined.zuora_renewal_subscription_name,
            zuora_subscription_spined.zuora_renewal_subscription_name_slugify,
            zuora_subscription_spined.current_term,
            zuora_subscription_spined.renewal_term,
            zuora_subscription_spined.renewal_term_period_type,
            zuora_subscription_spined.eoa_starter_bronze_offer_accepted,
            iff(
                zuora_subscription_spined.created_by_id
                -- All Self-Service / Web direct subscriptions are identified by that
                -- created_by_id
                = '2c92a0fd55822b4d015593ac264767f2',
                'Self-Service',
                'Sales-Assisted'
            ) as subscription_sales_type,

            -- Date Information
            zuora_subscription_spined.subscription_start_date
            as subscription_start_date,
            zuora_subscription_spined.subscription_end_date as subscription_end_date,
            date_trunc(
                'month', zuora_subscription_spined.subscription_start_date
            ) as subscription_start_month,
            date_trunc(
                'month', zuora_subscription_spined.subscription_end_date
            ) as subscription_end_month,
            snapshot_dates.fiscal_year as subscription_end_fiscal_year,
            zuora_subscription_spined.created_date::date as subscription_created_date,
            zuora_subscription_spined.updated_date::date as subscription_updated_date,
            zuora_subscription_spined.term_start_date::date as term_start_date,
            zuora_subscription_spined.term_end_date::date as term_end_date,
            date_trunc(
                'month', zuora_subscription_spined.term_start_date::date
            ) as term_start_month,
            date_trunc(
                'month', zuora_subscription_spined.term_end_date::date
            ) as term_end_month,
            case
                when
                    lower(zuora_subscription_spined.subscription_status) = 'active'
                    and zuora_subscription_spined.subscription_end_date > current_date
                then
                    date_trunc(
                        'month',
                        dateadd(
                            'month',
                            zuora_subscription_spined.current_term,
                            zuora_subscription_spined.subscription_end_date::date
                        )
                    )
                else null
            end as second_active_renewal_month,

            -- Lineage and Cohort Information
            subscription_lineage.subscription_lineage,
            subscription_lineage.oldest_subscription_in_cohort,
            subscription_lineage.subscription_cohort_month,
            subscription_lineage.subscription_cohort_quarter,
            subscription_lineage.subscription_cohort_year,

            -- Supersonics Fields
            zuora_subscription_spined.turn_on_cloud_licensing,
            zuora_subscription_spined.turn_on_operational_metrics,
            zuora_subscription_spined.contract_operational_metrics,
            zuora_subscription_spined.contract_auto_renewal,
            zuora_subscription_spined.turn_on_auto_renewal,
            zuora_subscription_spined.contract_seat_reconciliation,
            zuora_subscription_spined.turn_on_seat_reconciliation
        from zuora_subscription_spined
        inner join
            zuora_account_spined
            on zuora_subscription_spined.account_id = zuora_account_spined.account_id
            and zuora_subscription_spined.snapshot_id = zuora_account_spined.snapshot_id
        left join
            map_merged_crm_account
            on zuora_account_spined.crm_id = map_merged_crm_account.sfdc_account_id
        left join
            prep_amendment
            on zuora_subscription_spined.amendment_id = prep_amendment.dim_amendment_id
        left join
            subscription_lineage
            on subscription_lineage.subscription_name_slugify
            = zuora_subscription_spined.subscription_name_slugify
        left join
            snapshot_dates
            on zuora_subscription_spined.subscription_end_date::date
            = snapshot_dates.date_day

    ),
    final as (

        select
            {{ dbt_utils.surrogate_key(["snapshot_id", "dim_subscription_id"]) }}
            as subscription_snapshot_id,
            joined.*
        from joined

    )

    {{
        dbt_audit(
            cte_ref="final",
            created_by="@iweeks",
            updated_by="@jpeguero",
            created_date="2021-06-28",
            updated_date="2021-08-24",
        )
    }}
