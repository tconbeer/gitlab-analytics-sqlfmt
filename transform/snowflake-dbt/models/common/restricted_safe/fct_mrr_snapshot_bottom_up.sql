{{
    config(
        {
            "materialized": "incremental",
            "unique_key": "mrr_snapshot_id",
            "tags": ["arr_snapshots"],
        }
    )
}}

/* grain: one record per subscription, product per month */
with
    dim_date as (select * from {{ ref("dim_date") }}),
    prep_charge as (

        select
            prep_charge.*, charge_created_date as valid_from, '9999-12-31' as valid_to
        from {{ ref("prep_charge") }}
        where rate_plan_charge_name = 'manual true up allocation'

    ),
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
        -- Exclude Batch20 which are the test accounts. This method replaces the
        -- manual dbt seed exclusion file.
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
            /* This excludes Education customers (charge name EDU or OSS) with free subscriptions.
       Pull in seats from Paid EDU Plans with no ARR */
            charge_type = 'Recurring' and (
                mrr != 0 or lower(rate_plan_charge_name) = 'max enrollment'
            )

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
            is_deleted = false and exclude_from_analysis in (
                'False', ''
            ) and subscription_status not in ('Draft', 'Expired')

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
    manual_charges as (

        select
            date_id as snapshot_id,
            dim_charge_id,
            dim_billing_account_id,
            dim_crm_account_id,
            dim_subscription_id,
            subscription_name,
            subscription_name_slugify,
            subscription_status,
            dim_product_detail_id,
            mrr,
            delta_tcv,
            unit_of_measure,
            quantity,
            effective_start_month,
            effective_end_month
        from prep_charge
        inner join
            snapshot_dates
            on snapshot_dates.date_actual >= prep_charge.valid_from
            and snapshot_dates.date_actual
            < {{ coalesce_to_infinity("prep_charge.valid_to") }}

    ),
    non_manual_charges as (

        select
            zuora_rate_plan_charge_spined.snapshot_id,
            zuora_rate_plan_charge_spined.rate_plan_charge_id as dim_charge_id,
            zuora_account_spined.account_id as dim_billing_account_id,
            zuora_account_spined.crm_id as dim_crm_account_id,
            zuora_subscription_spined.subscription_id as dim_subscription_id,
            zuora_subscription_spined.subscription_name,
            zuora_subscription_spined.subscription_name_slugify,
            zuora_subscription_spined.subscription_status,
            zuora_rate_plan_charge_spined.product_rate_plan_charge_id
            as dim_product_detail_id,
            zuora_rate_plan_charge_spined.mrr,
            zuora_rate_plan_charge_spined.delta_tcv,
            zuora_rate_plan_charge_spined.unit_of_measure,
            zuora_rate_plan_charge_spined.quantity,
            zuora_rate_plan_charge_spined.effective_start_month,
            zuora_rate_plan_charge_spined.effective_end_month
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
    combined_charges as (

        select *
        from manual_charges

        union all

        select *
        from non_manual_charges

    ),
    mrr_month_by_month as (

        select
            snapshot_id,
            dim_date.date_id as dim_date_id,
            dim_charge_id,
            dim_billing_account_id,
            dim_crm_account_id,
            dim_subscription_id,
            dim_product_detail_id,
            subscription_name,
            subscription_name_slugify,
            subscription_status,
            sum(mrr) as mrr,
            sum(mrr) * 12 as arr,
            sum(quantity) as quantity,
            array_agg(unit_of_measure) as unit_of_measure
        from combined_charges
        inner join
            dim_date
            on combined_charges.effective_start_month <= dim_date.date_actual
            and (
                combined_charges.effective_end_month > dim_date.date_actual
                or combined_charges.effective_end_month is null
            ) and dim_date.day_of_month = 1
            {{ dbt_utils.group_by(n=10) }}

    ),
    final as (

        select
            {{
                dbt_utils.surrogate_key(
                    ["snapshot_id", "dim_date_id", "dim_charge_id"]
                )
            }} as mrr_snapshot_id,
            {{ dbt_utils.surrogate_key(["dim_date_id", "dim_charge_id"]) }} as mrr_id,
            snapshot_id,
            dim_date_id,
            dim_charge_id,
            dim_product_detail_id,
            dim_subscription_id,
            dim_billing_account_id,
            dim_crm_account_id,
            subscription_name,
            subscription_name_slugify,
            subscription_status,
            mrr,
            arr,
            quantity,
            unit_of_measure
        from mrr_month_by_month

    )

    {{
        dbt_audit(
            cte_ref="final",
            created_by="@iweeks",
            updated_by="@iweeks",
            created_date="2021-07-29",
            updated_date="2022-04-02",
        )
    }}
