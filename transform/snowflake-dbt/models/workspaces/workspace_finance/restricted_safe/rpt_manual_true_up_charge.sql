with
    dim_date as (select * from {{ ref("dim_date") }}),
    map_merged_crm_account as (select * from {{ ref("map_merged_crm_account") }}),
    zuora_account as (

        select *
        from {{ ref("zuora_account_source") }}
        where
            is_deleted = false
            -- Exclude Batch20 which are the test accounts. This method replaces the
            -- manual dbt seed exclusion file.
            and lower(batch) != 'batch20'

    ),
    zuora_rate_plan as (select * from {{ ref("zuora_rate_plan_source") }}),
    zuora_rate_plan_charge as (

        select *
        from {{ ref("zuora_rate_plan_charge_source") }}
        where charge_type = 'Recurring'

    ),
    zuora_subscription as (

        select *
        from {{ ref("zuora_subscription_source") }}
        where
            is_deleted = false
            and exclude_from_analysis in ('False', '')
            and subscription_status not in ('Draft')

    ),
    active_zuora_subscription as (

        select *
        from zuora_subscription
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
            zuora_account.account_id as dim_billing_account_id,
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
            zuora_account
            on revenue_contract_line.customer_number = zuora_account.account_number
        left join
            map_merged_crm_account
            on zuora_account.crm_id = map_merged_crm_account.sfdc_account_id
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
            lns.revenue_contract_id,
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
            {{ dbt_utils.group_by(n=8) }}

    ),
    manual_charges as (

        select
            revenue_contract_id,
            dim_billing_account_id,
            dim_crm_account_id,
            dim_charge_id,
            dim_subscription_id,
            subscription_name,
            subscription_status,
            dim_product_detail_id,
            adjustment,
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

    )

    {{
        dbt_audit(
            cte_ref="manual_charges",
            created_by="@michellecooper",
            updated_by="@michellecooper",
            created_date="2021-10-28",
            updated_date="2022-02-03",
        )
    }}
