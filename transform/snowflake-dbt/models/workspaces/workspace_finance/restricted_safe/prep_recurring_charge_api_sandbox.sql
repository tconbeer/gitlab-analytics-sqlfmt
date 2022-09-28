/* grain: one record per subscription per month */
with
    dim_date as (select * from {{ ref("dim_date") }}),
    map_merged_crm_account as (select * from {{ ref("map_merged_crm_account") }}),
    zuora_api_sandbox_account as (

        select *
        from {{ ref("zuora_api_sandbox_account_source") }}
        where is_deleted = false
    -- Keep the Batch20 test accounts since they would be in scope for this sandbox
    -- model.
    -- AND LOWER(batch) != 'batch20'
    ),
    zuora_api_sandbox_rate_plan as (

        select * from {{ ref("zuora_api_sandbox_rate_plan_source") }}

    ),
    zuora_api_sandbox_rate_plan_charge as (

        select *
        from {{ ref("zuora_api_sandbox_rate_plan_charge_source") }}
        where charge_type = 'Recurring'

    ),
    zuora_api_sandbox_subscription as (

        select *
        from {{ ref("zuora_api_sandbox_subscription_source") }}
        where
            is_deleted = false
            and exclude_from_analysis in ('False', '')
            and subscription_status not in ('Draft')

    ),
    active_zuora_subscription as (

        select *
        from zuora_api_sandbox_subscription
        where subscription_status in ('Active', 'Cancelled')

    ),
    manual_arr_true_up_allocation as (

        select * from {{ ref("sheetload_manual_arr_true_up_allocation_source") }}

    -- added as a work around until there is an automated method for adding true-up
    -- adjustments to Zuora Revenue/Zuora Billing
    ),
    manual_charges as (

        select
            manual_arr_true_up_allocation.account_id as billing_account_id,
            map_merged_crm_account.dim_crm_account_id as crm_account_id,
            md5(
                manual_arr_true_up_allocation.rate_plan_charge_id
            ) as rate_plan_charge_id,
            active_zuora_subscription.subscription_id as subscription_id,
            active_zuora_subscription.subscription_name as subscription_name,
            active_zuora_subscription.subscription_status as subscription_status,
            manual_arr_true_up_allocation.dim_product_detail_id as product_details_id,
            manual_arr_true_up_allocation.mrr as mrr,
            null as delta_tcv,
            manual_arr_true_up_allocation.unit_of_measure as unit_of_measure,
            0 as quantity,
            date_trunc('month', effective_start_date) as effective_start_month,
            date_trunc('month', effective_end_date) as effective_end_month
        from manual_arr_true_up_allocation
        inner join
            active_zuora_subscription
            on manual_arr_true_up_allocation.subscription_name
            = active_zuora_subscription.subscription_name
        inner join
            zuora_api_sandbox_account
            on active_zuora_subscription.account_id
            = zuora_api_sandbox_account.account_id
        left join
            map_merged_crm_account
            on zuora_api_sandbox_account.crm_id = map_merged_crm_account.sfdc_account_id

    ),
    rate_plan_charge_filtered as (

        select
            zuora_api_sandbox_account.account_id as billing_account_id,
            map_merged_crm_account.dim_crm_account_id as crm_account_id,
            zuora_api_sandbox_rate_plan_charge.rate_plan_charge_id,
            zuora_api_sandbox_subscription.subscription_id,
            zuora_api_sandbox_subscription.subscription_name,
            zuora_api_sandbox_subscription.subscription_status,
            zuora_api_sandbox_rate_plan_charge.product_rate_plan_charge_id
            as product_details_id,
            zuora_api_sandbox_rate_plan_charge.mrr,
            zuora_api_sandbox_rate_plan_charge.delta_tcv,
            zuora_api_sandbox_rate_plan_charge.unit_of_measure,
            zuora_api_sandbox_rate_plan_charge.quantity,
            zuora_api_sandbox_rate_plan_charge.effective_start_month,
            zuora_api_sandbox_rate_plan_charge.effective_end_month
        from zuora_api_sandbox_rate_plan_charge
        inner join
            zuora_api_sandbox_rate_plan
            on zuora_api_sandbox_rate_plan.rate_plan_id
            = zuora_api_sandbox_rate_plan_charge.rate_plan_id
        inner join
            zuora_api_sandbox_subscription
            on zuora_api_sandbox_rate_plan.subscription_id
            = zuora_api_sandbox_subscription.subscription_id
        inner join
            zuora_api_sandbox_account
            on zuora_api_sandbox_account.account_id
            = zuora_api_sandbox_subscription.account_id
        left join
            map_merged_crm_account
            on zuora_api_sandbox_account.crm_id = map_merged_crm_account.sfdc_account_id

    ),
    combined_rate_plans as (

        select *
        from rate_plan_charge_filtered

        union

        select *
        from manual_charges

    ),
    mrr_month_by_month as (

        select
            dim_date.date_id,
            billing_account_id,
            crm_account_id,
            subscription_id,
            subscription_name,
            subscription_status,
            product_details_id,
            rate_plan_charge_id,
            sum(mrr) as mrr,
            sum(mrr) * 12 as arr,
            sum(quantity) as quantity,
            array_agg(combined_rate_plans.unit_of_measure) as unit_of_measure
        from combined_rate_plans
        inner join
            dim_date
            on combined_rate_plans.effective_start_month <= dim_date.date_actual
            and (
                combined_rate_plans.effective_end_month > dim_date.date_actual
                or combined_rate_plans.effective_end_month is null
            )
            and dim_date.day_of_month = 1
            {{ dbt_utils.group_by(n=8) }}

    ),
    final as (

        select
            {{ dbt_utils.surrogate_key(["date_id", "rate_plan_charge_id"]) }} as mrr_id,
            date_id as dim_date_id,
            billing_account_id as dim_billing_account_id,
            crm_account_id as dim_crm_account_id,
            subscription_id as dim_subscription_id,
            product_details_id as dim_product_detail_id,
            rate_plan_charge_id as dim_charge_id,
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
            created_by="@ken_aguilar",
            updated_by="@ken_aguilar",
            created_date="2021-09-02",
            updated_date="2021-09-02",
        )
    }}
