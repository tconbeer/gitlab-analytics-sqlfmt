/* grain: one record per subscription per month */
{{
    simple_cte(
        [
            ("zuora_rate_plan", "zuora_rate_plan_source"),
            ("map_merged_crm_account", "map_merged_crm_account"),
            ("product_details", "dim_product_detail"),
            ("dim_date", "dim_date"),
        ]
    )
}},
zuora_account as (

    select *
    from {{ ref("zuora_account_source") }}
    -- Exclude Batch20 which are the test accounts. This method replaces the manual
    -- dbt seed exclusion file.
    where is_deleted = false and lower(batch) != 'batch20'

),
zuora_rate_plan_charge as (

    select *
    from {{ ref("zuora_rate_plan_charge_source") }}
    where charge_type = 'Recurring'

),
zuora_subscription as (

    select *
    from {{ ref("zuora_subscription_source") }}
    where is_deleted = false and exclude_from_analysis in ('False', '')

),
rate_plan_charge_filtered as (

    select
        zuora_account.account_id as dim_billing_account_id,
        map_merged_crm_account.dim_crm_account_id as dim_crm_account_id,
        zuora_subscription.subscription_id as dim_subscription_id,
        zuora_subscription.original_id as dim_subscription_id_original,
        zuora_subscription.subscription_status,
        zuora_rate_plan_charge.mrr,
        zuora_rate_plan_charge.unit_of_measure,
        zuora_rate_plan_charge.quantity,
        zuora_rate_plan_charge.effective_start_month,
        zuora_rate_plan_charge.effective_end_month,
        product_details.product_delivery_type
    from zuora_rate_plan_charge
    inner join
        zuora_rate_plan
        on zuora_rate_plan.rate_plan_id = zuora_rate_plan_charge.rate_plan_id
    inner join
        zuora_subscription
        on zuora_rate_plan.subscription_id = zuora_subscription.subscription_id
    inner join zuora_account on zuora_account.account_id = zuora_subscription.account_id
    left join
        map_merged_crm_account
        on zuora_account.crm_id = map_merged_crm_account.sfdc_account_id
    left join
        product_details
        on zuora_rate_plan_charge.product_rate_plan_charge_id
        = product_details.dim_product_detail_id

),
mrr_by_delivery_type as (

    select
        dim_date.date_id as dim_date_id,
        dim_date.first_day_of_month as charge_month,
        dim_billing_account_id,
        dim_crm_account_id,
        dim_subscription_id,
        dim_subscription_id_original,
        subscription_status,
        product_delivery_type,
        unit_of_measure,
        {{
            dbt_utils.surrogate_key(
                [
                    "dim_date_id",
                    "dim_subscription_id",
                    "product_delivery_type",
                    "unit_of_measure",
                ]
            )
        }} as mrr_id,
        sum(mrr) as mrr,
        sum(mrr) * 12 as arr,
        sum(quantity) as quantity
    from rate_plan_charge_filtered
    inner join
        dim_date
        on rate_plan_charge_filtered.effective_start_month <= dim_date.date_actual
        and (
            rate_plan_charge_filtered.effective_end_month > dim_date.date_actual
            or rate_plan_charge_filtered.effective_end_month is null
        )
        and dim_date.day_of_month = 1
        {{ dbt_utils.group_by(n=10) }}

),
mrr_by_subscription as (

    select
        subscription.dim_billing_account_id,
        subscription.dim_crm_account_id,
        subscription.dim_subscription_id,
        subscription.dim_subscription_id_original,
        subscription.subscription_status,
        subscription.dim_date_id,
        subscription.charge_month,
        sum(sm.mrr) as sm_mrr,
        sum(sm.arr) as sm_arr,
        sum(sm.quantity) as sm_quantity,
        sum(saas.mrr) as saas_mrr,
        sum(saas.arr) as saas_arr,
        sum(saas.quantity) as saas_quantity,
        sum(other.mrr) as other_mrr,
        sum(other.arr) as other_arr,
        sum(other.quantity) as other_quantity,
        sum(subscription.mrr) as total_mrr,
        sum(subscription.arr) as total_arr,
        sum(subscription.quantity) as total_quantity,
        array_agg(
            subscription.product_delivery_type || ': ' || subscription.unit_of_measure
        )
        within group(
            order by subscription.product_delivery_type desc
        ) as unit_of_measure
    from mrr_by_delivery_type subscription
    left join
        mrr_by_delivery_type sm
        on sm.product_delivery_type = 'Self-Managed'
        and subscription.mrr_id = sm.mrr_id
    left join
        mrr_by_delivery_type saas
        on saas.product_delivery_type = 'SaaS'
        and subscription.mrr_id = saas.mrr_id
    left join
        mrr_by_delivery_type other
        on other.product_delivery_type = 'Others'
        and subscription.mrr_id = other.mrr_id
        {{ dbt_utils.group_by(n=7) }}

),
final as (

    select
        dim_subscription_id,
        dim_subscription_id_original,
        dim_billing_account_id,
        dim_crm_account_id,
        dim_date_id,
        charge_month,
        subscription_status,
        unit_of_measure,
        total_mrr,
        total_arr,
        total_quantity,
        sm_mrr,
        sm_arr,
        sm_quantity,
        saas_mrr,
        saas_arr,
        saas_quantity,
        other_mrr,
        other_arr,
        other_quantity,
        iff(
            row_number() OVER (
                partition by dim_subscription_id order by dim_date_id desc
            )
            = 1,
            true,
            false
        ) as is_latest_record_per_subscription
    from mrr_by_subscription

)

{{
    dbt_audit(
        cte_ref="final",
        created_by="@ischweickartDD",
        updated_by="@iweeks",
        created_date="2021-03-01",
        updated_date="2021-07-29",
    )
}}
