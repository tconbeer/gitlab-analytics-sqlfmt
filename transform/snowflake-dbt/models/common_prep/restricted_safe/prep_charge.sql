with
    map_merged_crm_account as (select * from {{ ref("map_merged_crm_account") }}),
    sfdc_account as (

        select * from {{ ref("sfdc_account_source") }} where account_id is not null

    ),
    ultimate_parent_account as (

        select account_id
        from sfdc_account
        where account_id = ultimate_parent_account_id

    ),
    zuora_account as (

        select *
        from {{ ref("zuora_account_source") }}
        -- Exclude Batch20 which are the test accounts. This method replaces the
        -- manual dbt seed exclusion file.
        where is_deleted = false and lower(batch) != 'batch20'

    ),
    zuora_rate_plan as (select * from {{ ref("zuora_rate_plan_source") }}),
    zuora_rate_plan_charge as (

        select * from {{ ref("zuora_rate_plan_charge_source") }}

    ),
    zuora_subscription as (

        select *
        from {{ ref("zuora_subscription_source") }}
        where is_deleted = false and exclude_from_analysis in ('False', '')

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
            true_up_lines_dates.revenue_end_date as revenue_end_date,
            revenue_contract_line.revenue_contract_line_created_date
            as revenue_contract_line_created_date,
            revenue_contract_line.revenue_contract_line_updated_date
            as revenue_contract_line_updated_date
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
            lns.dim_billing_account_id,
            lns.dim_crm_account_id,
            lns.dim_charge_id,
            lns.dim_subscription_id,
            lns.subscription_name,
            lns.subscription_status,
            lns.dim_product_detail_id,
            min(
                lns.revenue_contract_line_created_date
            ) as revenue_contract_line_created_date,
            max(
                lns.revenue_contract_line_updated_date
            ) as revenue_contract_line_updated_date,
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
    non_manual_charges as (

        select
            -- Natural Key
            zuora_subscription.subscription_name,
            zuora_subscription.subscription_name_slugify,
            zuora_subscription.version as subscription_version,
            zuora_rate_plan_charge.rate_plan_charge_number,
            zuora_rate_plan_charge.version as rate_plan_charge_version,
            zuora_rate_plan_charge.segment as rate_plan_charge_segment,

            -- Surrogate Key
            zuora_rate_plan_charge.rate_plan_charge_id as dim_charge_id,

            -- Common Dimension Keys
            zuora_rate_plan_charge.product_rate_plan_charge_id as dim_product_detail_id,
            zuora_rate_plan.amendement_id as dim_amendment_id_charge,
            zuora_rate_plan.subscription_id as dim_subscription_id,
            zuora_rate_plan_charge.account_id as dim_billing_account_id,
            map_merged_crm_account.dim_crm_account_id as dim_crm_account_id,
            ultimate_parent_account.account_id as dim_parent_crm_account_id,
            {{ get_date_id("zuora_rate_plan_charge.effective_start_date") }}
            as effective_start_date_id,
            {{ get_date_id("zuora_rate_plan_charge.effective_end_date") }}
            as effective_end_date_id,

            -- Information
            zuora_subscription.subscription_status as subscription_status,
            zuora_rate_plan.rate_plan_name as rate_plan_name,
            zuora_rate_plan_charge.rate_plan_charge_name,
            zuora_rate_plan_charge.is_last_segment,
            zuora_rate_plan_charge.discount_level,
            zuora_rate_plan_charge.charge_type,
            zuora_rate_plan.amendement_type as rate_plan_charge_amendement_type,
            zuora_rate_plan_charge.unit_of_measure,
            case
                when
                    date_trunc('month', zuora_rate_plan_charge.charged_through_date)
                    = zuora_rate_plan_charge.effective_end_month::date
                then true
                else false
            end as is_paid_in_full,
            case
                when charged_through_date is null
                then zuora_subscription.current_term
                else
                    datediff(
                        'month',
                        date_trunc(
                            'month', zuora_rate_plan_charge.charged_through_date::date
                        ),
                        zuora_rate_plan_charge.effective_end_month::date
                    )
            end as months_of_future_billings,
            case
                when
                    effective_end_month > effective_start_month
                    or effective_end_month is null
                then true
                else false
            end as is_included_in_arr_calc,

            -- Dates
            zuora_subscription.subscription_end_date as subscription_end_date,
            zuora_rate_plan_charge.effective_start_date::date as effective_start_date,
            zuora_rate_plan_charge.effective_end_date::date as effective_end_date,
            zuora_rate_plan_charge.effective_start_month::date as effective_start_month,
            zuora_rate_plan_charge.effective_end_month::date as effective_end_month,
            zuora_rate_plan_charge.charged_through_date::date as charged_through_date,
            zuora_rate_plan_charge.created_date::date as charge_created_date,
            zuora_rate_plan_charge.updated_date::date as charge_updated_date,
            datediff(
                month,
                zuora_rate_plan_charge.effective_start_month::date,
                zuora_rate_plan_charge.effective_end_month::date
            ) as charge_term,

            -- Additive Fields
            zuora_rate_plan_charge.mrr,
            lag(zuora_rate_plan_charge.mrr, 1) over (
                partition by
                    zuora_subscription.subscription_name,
                    zuora_rate_plan_charge.rate_plan_charge_number
                order by zuora_rate_plan_charge.segment, zuora_subscription.version
            ) as previous_mrr_calc,
            case
                when previous_mrr_calc is null then 0 else previous_mrr_calc
            end as previous_mrr,
            zuora_rate_plan_charge.mrr - previous_mrr as delta_mrr_calc,
            case
                when
                    lower(subscription_status) = 'active'
                    and subscription_end_date <= current_date
                    and is_last_segment = true
                then - previous_mrr
                when lower(subscription_status) = 'cancelled' and is_last_segment = true
                then - previous_mrr
                else delta_mrr_calc
            end as delta_mrr,
            zuora_rate_plan_charge.delta_mrc,
            zuora_rate_plan_charge.mrr * 12 as arr,
            previous_mrr * 12 as previous_arr,
            zuora_rate_plan_charge.delta_mrc * 12 as delta_arc,
            delta_mrr * 12 as delta_arr,
            zuora_rate_plan_charge.quantity,
            lag(zuora_rate_plan_charge.quantity, 1) over (
                partition by
                    zuora_subscription.subscription_name,
                    zuora_rate_plan_charge.rate_plan_charge_number
                order by zuora_rate_plan_charge.segment, zuora_subscription.version
            ) as previous_quantity_calc,
            case
                when previous_quantity_calc is null then 0 else previous_quantity_calc
            end as previous_quantity,
            zuora_rate_plan_charge.quantity - previous_quantity as delta_quantity_calc,
            case
                when
                    lower(subscription_status) = 'active'
                    and subscription_end_date <= current_date
                    and is_last_segment = true
                then - previous_quantity
                when lower(subscription_status) = 'cancelled' and is_last_segment = true
                then - previous_quantity
                else delta_quantity_calc
            end as delta_quantity,
            zuora_rate_plan_charge.tcv,
            zuora_rate_plan_charge.delta_tcv,
            case
                when is_paid_in_full = false
                then months_of_future_billings * zuora_rate_plan_charge.mrr
                else 0
            end as estimated_total_future_billings

        from zuora_rate_plan
        inner join
            zuora_rate_plan_charge
            on zuora_rate_plan.rate_plan_id = zuora_rate_plan_charge.rate_plan_id
        inner join
            zuora_subscription
            on zuora_rate_plan.subscription_id = zuora_subscription.subscription_id
        inner join
            zuora_account on zuora_subscription.account_id = zuora_account.account_id
        left join
            map_merged_crm_account
            on zuora_account.crm_id = map_merged_crm_account.sfdc_account_id
        left join
            sfdc_account
            on map_merged_crm_account.dim_crm_account_id = sfdc_account.account_id
        left join
            ultimate_parent_account
            on sfdc_account.ultimate_parent_account_id
            = ultimate_parent_account.account_id

    ),
    manual_charges_prep as (

        select
            dim_billing_account_id,
            dim_crm_account_id,
            dim_charge_id,
            dim_subscription_id,
            subscription_name,
            subscription_status,
            dim_product_detail_id,
            revenue_contract_line_created_date,
            revenue_contract_line_updated_date,
            adjustment / round(
                months_between(revenue_end_date::date, revenue_start_date::date), 0
            ) as mrr,
            null as delta_tcv,
            'Seats' as unit_of_measure,
            0 as quantity,
            revenue_start_date::date as effective_start_date,
            dateadd('day', 1, revenue_end_date::date) as effective_end_date
        from true_up_lines_subcription_grain

    ),
    manual_charges as (

        select
            active_zuora_subscription.subscription_name as subscription_name,
            active_zuora_subscription.subscription_name_slugify
            as subscription_name_slugify,
            active_zuora_subscription.version as subscription_version,
            null as rate_plan_charge_number,
            null as rate_plan_charge_version,
            null as rate_plan_charge_segment,
            manual_charges_prep.dim_charge_id as dim_charge_id,
            manual_charges_prep.dim_product_detail_id as dim_product_detail_id,
            null as dim_amendment_id_charge,
            active_zuora_subscription.subscription_id as dim_subscription_id,
            manual_charges_prep.dim_billing_account_id as dim_billing_account_id,
            zuora_account.crm_id as dim_crm_account_id,
            sfdc_account.ultimate_parent_account_id as dim_parent_crm_account_id,
            {{ get_date_id("manual_charges_prep.effective_start_date") }}
            as effective_start_date_id,
            {{ get_date_id("manual_charges_prep.effective_end_date") }}
            as effective_end_date_id,
            active_zuora_subscription.subscription_status as subscription_status,
            'manual true up allocation' as rate_plan_name,
            'manual true up allocation' as rate_plan_charge_name,
            'TRUE' as is_last_segment,
            null as discount_level,
            'Recurring' as charge_type,
            null as rate_plan_charge_amendement_type,
            manual_charges_prep.unit_of_measure as unit_of_measure,
            'TRUE' as is_paid_in_full,
            active_zuora_subscription.current_term as months_of_future_billings,
            case
                when
                    date_trunc('month', effective_end_date)
                    > date_trunc('month', effective_start_date)
                    or date_trunc('month', effective_end_date) is null
                then true
                else false
            end as is_included_in_arr_calc,
            active_zuora_subscription.subscription_end_date as subscription_end_date,
            effective_start_date as effective_start_date,
            effective_end_date as effective_end_date,
            date_trunc('month', effective_start_date) as effective_start_month,
            date_trunc('month', effective_end_date) as effective_end_month,
            dateadd('day', 1, effective_end_date) as charged_through_date,
            revenue_contract_line_created_date as charge_created_date,
            revenue_contract_line_updated_date as charge_updated_date,
            datediff(
                'month', effective_start_month::date, effective_end_month::date
            ) as charge_term,
            manual_charges_prep.mrr as mrr,
            null as previous_mrr_calc,
            null as previous_mrr,
            null as delta_mrr_calc,
            null as delta_mrr,
            null as delta_mrc,
            manual_charges_prep.mrr * 12 as arr,
            null as previous_arr,
            null as delta_arc,
            null as delta_arr,
            0 as quantity,
            null as previous_quantity_calc,
            null as previous_quantity,
            null as delta_quantity_calc,
            null as delta_quantity,
            null as tcv,
            null as delta_tcv,
            case
                when is_paid_in_full = false
                then months_of_future_billings * manual_charges_prep.mrr
                else 0
            end as estimated_total_future_billings
        from manual_charges_prep
        inner join
            active_zuora_subscription
            on manual_charges_prep.subscription_name
            = active_zuora_subscription.subscription_name
        inner join
            zuora_account
            on active_zuora_subscription.account_id = zuora_account.account_id
        left join
            map_merged_crm_account
            on zuora_account.crm_id = map_merged_crm_account.sfdc_account_id
        left join
            sfdc_account
            on map_merged_crm_account.dim_crm_account_id = sfdc_account.account_id
        left join
            ultimate_parent_account
            on sfdc_account.ultimate_parent_account_id
            = ultimate_parent_account.account_id

    ),
    combined_charges as (

        select *
        from non_manual_charges

        union

        select *
        from manual_charges

    ),
    arr_analysis_framework as (

        select
            combined_charges.*,
            case
                when subscription_version = 1
                then 'New'
                when
                    lower(subscription_status) = 'active'
                    and subscription_end_date <= current_date
                then 'Churn'
                when lower(subscription_status) = 'cancelled'
                then 'Churn'
                when arr < previous_arr and arr > 0
                then 'Contraction'
                when arr > previous_arr and subscription_version > 1
                then 'Expansion'
                when arr = previous_arr
                then 'No Impact'
                else null
            end as type_of_arr_change
        from combined_charges

    )

    {{
        dbt_audit(
            cte_ref="arr_analysis_framework",
            created_by="@iweeks",
            updated_by="@michellecooper",
            created_date="2021-04-28",
            updated_date="2022-02-03",
        )
    }}
