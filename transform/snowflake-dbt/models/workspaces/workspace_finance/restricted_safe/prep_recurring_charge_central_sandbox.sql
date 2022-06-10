/* grain: one record per subscription per month */
with
    dim_date as (select * from {{ ref("dim_date") }}),
    map_merged_crm_account as (select * from {{ ref("map_merged_crm_account") }}),
    zuora_central_sandbox_account as (

        select *
        from {{ ref("zuora_central_sandbox_account_source") }}
        where is_deleted = false
    -- Keep the Batch20 test accounts since they would be in scope for this sandbox
    -- model.
    -- AND LOWER(batch) != 'batch20'
    ),
    zuora_central_sandbox_rate_plan as (

        select * from {{ ref("zuora_central_sandbox_rate_plan_source") }}

    ),
    zuora_central_sandbox_rate_plan_charge as (

        select *
        from {{ ref("zuora_central_sandbox_rate_plan_charge_source") }}
        where charge_type = 'Recurring'

    ),
    zuora_central_sandbox_subscription as (

        select *
        from {{ ref("zuora_central_sandbox_subscription_source") }}
        where
            is_deleted = false and exclude_from_analysis in (
                'False', ''
            ) and subscription_status not in ('Draft')

    ),
    active_zuora_subscription as (

        select *
        from zuora_central_sandbox_subscription
        where subscription_status in ('Active', 'Cancelled')

    ),
    revenue_contract_line as (

        select * from {{ ref("zuora_revenue_revenue_contract_line_source") }}

    ),
    mje as (

        select
            *,
            case
                when
                    debit_activity_type = 'Revenue'
                    and credit_activity_type = 'Contract Liability'
                then - amount
                when
                    credit_activity_type = 'Revenue'
                    and debit_activity_type = 'Contract Liability'
                then amount
                else amount
            end as adjustment_amount
        from {{ ref("zuora_revenue_manual_journal_entry_source") }}

    ),
    true_up_lines_dates as (

        select
            subscription_name,
            revenue_contract_line_attribute_16,
            min(revenue_start_date) as revenue_start_date,
            max(revenue_end_date) as revenue_end_date
        from revenue_contract_line
        group by 1, 2

    ),
    true_up_lines as (

        select
            revenue_contract_line_id,
            revenue_contract_id,
            zuora_central_sandbox_account.account_id as dim_billing_account_id,
            map_merged_crm_account.dim_crm_account_id as dim_crm_account_id,
            md5(rate_plan_charge_id) as dim_charge_id,
            active_zuora_subscription.subscription_id as dim_subscription_id,
            active_zuora_subscription.subscription_name as subscription_name,
            active_zuora_subscription.subscription_status as subscription_status,
            product_rate_plan_charge_id as dim_product_detail_id,
            true_up_lines_dates.revenue_start_date as revenue_start_date,
            true_up_lines_dates.revenue_end_date as revenue_end_date
        from revenue_contract_line
        inner join
            active_zuora_subscription
            on revenue_contract_line.subscription_name
            = active_zuora_subscription.subscription_name
        inner join
            zuora_central_sandbox_account
            on revenue_contract_line.customer_number
            = zuora_central_sandbox_account.account_number
        left join
            map_merged_crm_account
            on zuora_central_sandbox_account.crm_id
            = map_merged_crm_account.sfdc_account_id
        left join
            true_up_lines_dates
            on revenue_contract_line.subscription_name
            = true_up_lines_dates.subscription_name
            and revenue_contract_line.revenue_contract_line_attribute_16
            = true_up_lines_dates.revenue_contract_line_attribute_16
        where
            revenue_contract_line.revenue_contract_line_attribute_16
            like '%True-up ARR Allocation%'

    ),
    mje_summed as (

        select mje.revenue_contract_line_id, sum(adjustment_amount) as adjustment
        from mje
        inner join
            true_up_lines
            on mje.revenue_contract_line_id = true_up_lines.revenue_contract_line_id
            and mje.revenue_contract_id = true_up_lines.revenue_contract_id
            {{ dbt_utils.group_by(n=1) }}

    ),
    true_up_lines_subcription_grain as (

        select
            lns.dim_billing_account_id,
            lns.dim_crm_account_id,
            lns.dim_charge_id,
            lns.dim_subscription_id,
            lns.subscription_name,
            lns.subscription_status,
            lns.dim_product_detail_id,
            sum(mje.adjustment) as adjustment,
            min(revenue_start_date) as revenue_start_date,
            max(revenue_end_date) as revenue_end_date
        from true_up_lines lns
        left join
            mje_summed mje
            on lns.revenue_contract_line_id = mje.revenue_contract_line_id
        where
            adjustment is not null and abs(round(adjustment, 5)) > 0
            {{ dbt_utils.group_by(n=7) }}

    ),
    manual_charges as (

        select
            dim_billing_account_id,
            dim_crm_account_id,
            dim_charge_id,
            dim_subscription_id,
            subscription_name,
            subscription_status,
            dim_product_detail_id,
            adjustment / round(
                months_between(revenue_end_date::date, revenue_start_date::date), 0
            ) as mrr,
            null as delta_tcv,
            'Seats' as unit_of_measure,
            0 as quantity,
            date_trunc('month', revenue_start_date::date) as effective_start_month,
            date_trunc(
                'month', dateadd('day', 1, revenue_end_date::date)
            ) as effective_end_month
        from true_up_lines_subcription_grain

    ),
    rate_plan_charge_filtered as (

        select
            zuora_central_sandbox_account.account_id as billing_account_id,
            map_merged_crm_account.dim_crm_account_id as crm_account_id,
            zuora_central_sandbox_rate_plan_charge.rate_plan_charge_id,
            zuora_central_sandbox_subscription.subscription_id,
            zuora_central_sandbox_subscription.subscription_name,
            zuora_central_sandbox_subscription.subscription_status,
            zuora_central_sandbox_rate_plan_charge.product_rate_plan_charge_id
            as product_details_id,
            zuora_central_sandbox_rate_plan_charge.mrr,
            zuora_central_sandbox_rate_plan_charge.delta_tcv,
            zuora_central_sandbox_rate_plan_charge.unit_of_measure,
            zuora_central_sandbox_rate_plan_charge.quantity,
            zuora_central_sandbox_rate_plan_charge.effective_start_month,
            zuora_central_sandbox_rate_plan_charge.effective_end_month
        from zuora_central_sandbox_rate_plan_charge
        inner join
            zuora_central_sandbox_rate_plan
            on zuora_central_sandbox_rate_plan.rate_plan_id
            = zuora_central_sandbox_rate_plan_charge.rate_plan_id
        inner join
            zuora_central_sandbox_subscription
            on zuora_central_sandbox_rate_plan.subscription_id
            = zuora_central_sandbox_subscription.subscription_id
        inner join
            zuora_central_sandbox_account
            on zuora_central_sandbox_account.account_id
            = zuora_central_sandbox_subscription.account_id
        left join
            map_merged_crm_account
            on zuora_central_sandbox_account.crm_id
            = map_merged_crm_account.sfdc_account_id

    ),
    combined_rate_plans as (

        select * from rate_plan_charge_filtered UNION select * from manual_charges

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
            ) and dim_date.day_of_month = 1
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
            created_by="@michellecooper",
            updated_by="@michellecooper",
            created_date="2022-03-31",
            updated_date="2022-03-31",
        )
    }}
