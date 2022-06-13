{{
    config(
        {
            "materialized": "incremental",
            "unique_key": "charge_snapshot_id",
            "tags": ["arr_snapshots"],
        }
    )
}}

/* grain: one record per subscription, product charge per month */
with
    dim_date as (select * from {{ ref("dim_date") }}),
    snapshot_dates as (

        select *
        from {{ ref("dim_date") }}
        where
            date_actual >= '2020-03-01' and date_actual <= current_date

            {% if is_incremental() %}

            -- this filter will only be applied on an incremental run
            and date_id > (select max(snapshot_id) from {{ this }})

            {% endif %}

    ),
    zuora_account as (

        select *
        from {{ ref("zuora_account_snapshots_source") }}
        where is_deleted = false

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
    zuora_rate_plan as (select * from {{ ref("zuora_rate_plan_snapshots_source") }}),
    zuora_rate_plan_spined as (

        select snapshot_dates.date_id as snapshot_id, zuora_rate_plan.*
        from zuora_rate_plan
        inner join
            snapshot_dates
            on snapshot_dates.date_actual >= zuora_rate_plan.dbt_valid_from
            and snapshot_dates.date_actual
            < {{ coalesce_to_infinity("zuora_rate_plan.dbt_valid_to") }}

    ),
    zuora_rate_plan_charge as (

        select *
        from {{ ref("zuora_rate_plan_charge_snapshots_source") }}
        where
            charge_type = 'Recurring'
            /* This excludes Education customers (charge name EDU or OSS) with free subscriptions */
            and mrr != 0
            /* Only include charges that have effective dates in 2 or more months. This aligns to the ARR calc used in mart_arr */
            and effective_end_month > effective_start_month

    ),
    zuora_rate_plan_charge_spined as (

        select snapshot_dates.date_id as snapshot_id, zuora_rate_plan_charge.*
        from zuora_rate_plan_charge
        inner join
            snapshot_dates
            on snapshot_dates.date_actual >= zuora_rate_plan_charge.dbt_valid_from
            and snapshot_dates.date_actual
            < {{ coalesce_to_infinity("zuora_rate_plan_charge.dbt_valid_to") }}

    ),
    zuora_subscription as (

        select *
        from {{ ref("zuora_subscription_snapshots_source") }}
        where
            subscription_status not in (
                'Draft', 'Expired'
            ) and is_deleted = false and exclude_from_analysis in ('False', '')

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
            ) = 1

    ),
    rate_plan_charge_filtered as (

        select
            zuora_rate_plan_charge_spined.snapshot_id,
            zuora_account_spined.account_id as dim_billing_account_id,
            zuora_account_spined.crm_id as dim_crm_account_id,
            zuora_rate_plan_charge_spined.rate_plan_charge_id as charge_id,
            zuora_rate_plan_charge_spined.rate_plan_charge_name,
            zuora_subscription_spined.subscription_id as dim_subscription_id,
            zuora_subscription_spined.subscription_name,
            zuora_subscription_spined.subscription_status,
            zuora_subscription_spined.version as subscription_version,
            zuora_subscription_spined.current_term,
            zuora_rate_plan_charge_spined.product_rate_plan_charge_id
            as dim_product_detail_id,
            zuora_rate_plan_charge_spined.mrr,
            zuora_rate_plan_charge_spined.delta_mrc as delta_mrr,
            zuora_rate_plan_charge_spined.unit_of_measure,
            zuora_rate_plan_charge_spined.quantity,
            zuora_rate_plan_charge_spined.charge_type,
            zuora_rate_plan_charge_spined.charged_through_date,
            zuora_rate_plan_charge_spined.rate_plan_charge_number,
            zuora_rate_plan_charge_spined.segment as charge_segment,
            zuora_rate_plan_charge_spined.version as charge_version,
            date_trunc(
                'month', zuora_subscription_spined.subscription_start_date::date
            ) as subscription_start_month,
            date_trunc(
                'month', zuora_subscription_spined.subscription_end_date::date
            ) as subscription_end_month,
            zuora_subscription_spined.subscription_start_date::date
            as subscription_start_date,
            zuora_subscription_spined.subscription_end_date::date
            as subscription_end_date,
            zuora_rate_plan_charge_spined.effective_start_month,
            zuora_rate_plan_charge_spined.effective_end_month,
            zuora_rate_plan_charge_spined.effective_start_date::date
            as effective_start_date,
            zuora_rate_plan_charge_spined.effective_end_date::date as effective_end_date
        from zuora_rate_plan_charge_spined
        inner join
            zuora_rate_plan_spined
            on zuora_rate_plan_spined.rate_plan_id
            = zuora_rate_plan_charge_spined.rate_plan_id
            and zuora_rate_plan_spined.snapshot_id
            = zuora_rate_plan_charge_spined.snapshot_id
        inner join
            zuora_subscription_spined
            on zuora_rate_plan_spined.subscription_id
            = zuora_subscription_spined.subscription_id
            and zuora_rate_plan_spined.snapshot_id
            = zuora_subscription_spined.snapshot_id
        inner join
            zuora_account_spined
            on zuora_account_spined.account_id = zuora_subscription_spined.account_id
            and zuora_account_spined.snapshot_id = zuora_subscription_spined.snapshot_id

    ),
    charges_day_by_day as (

        select
            dim_date.date_actual as snapshot_date,
            snapshot_id,
            dim_billing_account_id,
            dim_crm_account_id,
            charge_id,
            dim_subscription_id,
            dim_product_detail_id,
            subscription_start_month,
            subscription_end_month,
            subscription_start_date,
            subscription_end_date,
            effective_start_month,
            effective_end_month,
            effective_start_date,
            effective_end_date,
            subscription_name,
            subscription_status,
            subscription_version,
            current_term,
            rate_plan_charge_name,
            charge_type,
            charged_through_date,
            rate_plan_charge_number,
            charge_segment,
            charge_version,
            sum(delta_mrr) as delta_mrr,
            sum(mrr) as mrr,
            sum(delta_mrr) * 12 as delta_arr,
            sum(mrr) * 12 as arr,
            sum(quantity) as quantity,
            array_agg(rate_plan_charge_filtered.unit_of_measure) as unit_of_measure
        from rate_plan_charge_filtered
        inner join
            dim_date on rate_plan_charge_filtered.snapshot_id = dim_date.date_id
            {{ dbt_utils.group_by(n=25) }}

    ),
    final as (

        select
            {{
                dbt_utils.surrogate_key(
                    [
                        "snapshot_id",
                        "subscription_name",
                        "dim_product_detail_id",
                        "charge_id",
                    ]
                )
            }} as charge_snapshot_id, charges_day_by_day.*
        from charges_day_by_day
        order by snapshot_date desc, subscription_name, rate_plan_charge_name

    )

    {{
        dbt_audit(
            cte_ref="final",
            created_by="@iweeks",
            updated_by="@iweeks",
            created_date="2021-02-13",
            updated_date="2020-03-15",
        )
    }}
