with
    dim_billing_account as (select * from {{ ref("dim_billing_account") }}),
    dim_crm_account as (select * from {{ ref("dim_crm_account") }}),
    dim_crm_opportunity as (select * from {{ ref("dim_crm_opportunity") }}),
    dim_date as (select * from {{ ref("dim_date") }}),
    dim_product_detail as (select * from {{ ref("dim_product_detail") }}),
    dim_quote as (select * from {{ ref("dim_quote") }}),
    fct_charge as (

        select *
        from {{ ref("wk_finance_fct_recurring_charge_daily_snapshot") }}
        where snapshot_date = '2021-02-04'

    ),
    fct_quote_item as (select * from {{ ref("fct_quote_item") }}),
    zuora_subscription as (

        select *
        from {{ ref("zuora_subscription_snapshots_source") }}
        where
            subscription_status not in ('Draft', 'Expired')
            and is_deleted = false
            and exclude_from_analysis in ('False', '')

    ),
    zuora_subscription_spined as (

        select snapshot_dates.date_id as snapshot_id, zuora_subscription.*
        from zuora_subscription
        inner join
            dim_date snapshot_dates
            on snapshot_dates.date_actual >= zuora_subscription.dbt_valid_from
            and snapshot_dates.date_actual
            < {{ coalesce_to_infinity("zuora_subscription.dbt_valid_to") }}
            and date_actual = '2021-02-04'
        qualify
            rank() over (
                partition by subscription_name, snapshot_dates.date_actual
                order by dbt_valid_from desc
            )
            = 1

    ),
    opportunity as (

        select distinct
            dim_crm_opportunity.dim_crm_opportunity_id,
            fct_quote_item.dim_subscription_id
        from fct_quote_item
        inner join
            dim_crm_opportunity
            on fct_quote_item.dim_crm_opportunity_id
            = dim_crm_opportunity.dim_crm_opportunity_id
        inner join dim_quote on fct_quote_item.dim_quote_id = dim_quote.dim_quote_id
        where stage_name in ('Closed Won', '8-Closed Lost') and is_primary_quote = true

    ),
    renewal_subscriptions as (

        select distinct
            sub_1.subscription_name,
            sub_1.zuora_renewal_subscription_name,
            date_trunc(
                'month', sub_2.subscription_end_date::date
            ) as subscription_end_month,
            rank() over (
                partition by sub_1.subscription_name
                order by
                    sub_1.zuora_renewal_subscription_name, sub_2.subscription_end_date
            ) as rank
        from zuora_subscription_spined sub_1
        inner join
            zuora_subscription_spined sub_2
            on sub_1.zuora_renewal_subscription_name = sub_2.subscription_name
            and date_trunc('month', sub_2.subscription_end_date) >= '2022-02-01'
        where sub_1.zuora_renewal_subscription_name != ''
        qualify rank = 1

    ),
    base as (  -- get the base data set of recurring charges.

        select
            fct_charge.charge_id,
            fct_charge.dim_crm_account_id,
            fct_charge.dim_billing_account_id,
            opportunity.dim_crm_opportunity_id,
            fct_charge.dim_subscription_id,
            fct_charge.dim_product_detail_id,
            dim_crm_account.parent_crm_account_name,
            dim_crm_account.crm_account_name,
            dim_crm_account.parent_crm_account_sales_segment,
            dim_product_detail.product_tier_name,
            dim_product_detail.product_delivery_type,
            dim_subscription.subscription_name,
            dim_subscription.zuora_renewal_subscription_name,
            dim_subscription.current_term,
            case
                when dim_subscription.current_term >= 24
                then true
                when
                    dim_subscription.subscription_name
                    in (select distinct subscription_name from renewal_subscriptions)
                then true
                else false
            end as is_myb,
            case
                when
                    dim_subscription.subscription_name
                    in (select distinct subscription_name from renewal_subscriptions)
                then true
                else false
            end as is_myb_with_multi_subs,
            case
                when
                    date_trunc('month', fct_charge.charged_through_date)
                    = fct_charge.effective_end_month
                then true
                else false
            end as is_paid_in_full,
            case
                when charged_through_date is null
                then dim_subscription.current_term
                else
                    datediff(
                        'month',
                        date_trunc('month', fct_charge.charged_through_date),
                        fct_charge.effective_end_month
                    )
            end as months_of_future_billings,
            case
                when is_paid_in_full = false
                then months_of_future_billings * fct_charge.mrr
                else 0
            end as estimated_total_future_billings,
            case
                when
                    is_paid_in_full = false
                    and fct_charge.effective_end_month <= '2022-01-01'
                    and is_myb = true
                then months_of_future_billings * fct_charge.mrr
                else 0
            end as estimated_fy22_future_billings,
            fct_charge.effective_start_month,
            fct_charge.effective_end_month,
            fct_charge.subscription_start_month,
            fct_charge.subscription_end_month,
            renewal_subscriptions.subscription_end_month as myb_subscription_end_month,
            datediff(
                month, fct_charge.effective_start_month, fct_charge.effective_end_month
            ) as charge_term,
            fct_charge.arr
        from fct_charge
        left join
            zuora_subscription_spined dim_subscription
            on fct_charge.dim_subscription_id = dim_subscription.subscription_id
        left join
            dim_billing_account
            on fct_charge.dim_billing_account_id
            = dim_billing_account.dim_billing_account_id
        left join
            opportunity
            on fct_charge.dim_subscription_id = opportunity.dim_subscription_id
        left join
            dim_crm_account
            on fct_charge.dim_crm_account_id = dim_crm_account.dim_crm_account_id
        left join
            renewal_subscriptions
            on fct_charge.subscription_name = renewal_subscriptions.subscription_name
        left join
            dim_product_detail
            on fct_charge.dim_product_detail_id
            = dim_product_detail.dim_product_detail_id
        where
            fct_charge.effective_start_month <= '2021-01-01'
            and fct_charge.effective_end_month > '2021-01-01'

    ),
    agg_charge_term_less_than_equal_12 as (  -- get the starting and ending month ARR for charges with charge terms <= 12 months. These charges do not need additional logic.

        select
            case when is_myb = true then 'MYB' else 'Non-MYB' end as renewal_type,
            is_myb,
            is_myb_with_multi_subs,
            current_term,
            charge_term,
            charge_id,
            dim_crm_account_id,
            dim_billing_account_id,
            dim_crm_opportunity_id,
            dim_subscription_id,
            dim_product_detail_id,
            product_tier_name,
            product_delivery_type,
            subscription_name,
            effective_start_month,
            effective_end_month,
            subscription_end_month,
            myb_subscription_end_month,
            sum(arr) as arr
        from base
        where charge_term <= 12 {{ dbt_utils.group_by(n=18) }}

    ),
    agg_charge_term_greater_than_12 as (  -- get the starting and ending month ARR for charges with charge terms > 12 months. These charges need additional logic.

        select
            case when is_myb = true then 'MYB' else 'Non-MYB' end as renewal_type,
            is_myb,
            is_myb_with_multi_subs,
            current_term,
            case  -- the below odd term charges do not behave well in the MYB logic and end up with duplicate renewals in the fiscal year. This CASE statement smooths out the charges so they only have one renewal entry in the fiscal year.
                when charge_term = 26
                then 24
                when charge_term = 28
                then 24
                when charge_term = 38
                then 36
                when charge_term = 57
                then 60
                else charge_term
            end as charge_term,
            charge_id,
            dim_crm_account_id,
            dim_billing_account_id,
            dim_crm_opportunity_id,
            dim_subscription_id,
            dim_product_detail_id,
            product_tier_name,
            product_delivery_type,
            subscription_name,
            effective_start_month,
            effective_end_month,
            subscription_end_month,
            myb_subscription_end_month,
            sum(arr) as arr
        from base
        where charge_term > 12 {{ dbt_utils.group_by(n=18) }}

    ),
    twenty_four_mth_term as (  -- create records for the intermitent renewals for multi-year charges that are not in the Zuora data. The start and end months are in the agg_myb for MYB.

        select
            renewal_type,
            is_myb,
            is_myb_with_multi_subs,
            current_term,
            charge_term,
            charge_id,
            dim_crm_account_id,
            dim_billing_account_id,
            dim_crm_opportunity_id,
            dim_subscription_id,
            dim_product_detail_id,
            product_tier_name,
            product_delivery_type,
            subscription_name,
            effective_start_month,
            dateadd(
                'month', charge_term / 2, effective_start_month
            ) as effective_end_month,
            subscription_end_month,
            myb_subscription_end_month,
            sum(arr) as arr
        from agg_charge_term_greater_than_12
        where
            charge_term between 13 and 24
            and effective_end_month > '2022-01-01' {{ dbt_utils.group_by(n=18) }}

    ),
    thirty_six_mth_term as (  -- create records for the intermitent renewals for MYBs that are not in the Zuora data. The start and end months are in the agg_myb for MYBs.

        select
            renewal_type,
            is_myb,
            is_myb_with_multi_subs,
            current_term,
            charge_term,
            charge_id,
            dim_crm_account_id,
            dim_billing_account_id,
            dim_crm_opportunity_id,
            dim_subscription_id,
            dim_product_detail_id,
            product_tier_name,
            product_delivery_type,
            subscription_name,
            effective_start_month,
            dateadd(
                'month', charge_term / 3, effective_start_month
            ) as effective_end_month,
            subscription_end_month,
            myb_subscription_end_month,
            sum(arr) as arr
        from agg_charge_term_greater_than_12
        where
            charge_term between 25 and 36
            and effective_end_month > '2022-01-01' {{ dbt_utils.group_by(n=18) }}
        union all
        select
            renewal_type,
            is_myb,
            is_myb_with_multi_subs,
            current_term,
            charge_term,
            charge_id,
            dim_crm_account_id,
            dim_billing_account_id,
            dim_crm_opportunity_id,
            dim_subscription_id,
            dim_product_detail_id,
            product_tier_name,
            product_delivery_type,
            subscription_name,
            effective_start_month,
            dateadd(
                'month', charge_term / 3 * 2, effective_start_month
            ) as effective_end_month,
            subscription_end_month,
            myb_subscription_end_month,
            sum(arr) as arr
        from agg_charge_term_greater_than_12
        where
            charge_term between 25 and 36
            and effective_end_month > '2022-01-01' {{ dbt_utils.group_by(n=18) }}
        order by 1

    ),
    forty_eight_mth_term as (  -- create records for the intermitent renewals for MYBs that are not in the Zuora data. The start and end months are in the agg_myb for MYBs.

        select
            renewal_type,
            is_myb,
            is_myb_with_multi_subs,
            current_term,
            charge_term,
            charge_id,
            dim_crm_account_id,
            dim_billing_account_id,
            dim_crm_opportunity_id,
            dim_subscription_id,
            dim_product_detail_id,
            product_tier_name,
            product_delivery_type,
            subscription_name,
            effective_start_month,
            dateadd(
                'month', charge_term / 4, effective_start_month
            ) as effective_end_month,
            subscription_end_month,
            myb_subscription_end_month,
            sum(arr) as arr
        from agg_charge_term_greater_than_12
        where
            charge_term between 37 and 48
            and effective_end_month > '2022-01-01' {{ dbt_utils.group_by(n=18) }}
        union all
        select
            renewal_type,
            is_myb,
            is_myb_with_multi_subs,
            current_term,
            charge_term,
            charge_id,
            dim_crm_account_id,
            dim_billing_account_id,
            dim_crm_opportunity_id,
            dim_subscription_id,
            dim_product_detail_id,
            product_tier_name,
            product_delivery_type,
            subscription_name,
            effective_start_month,
            dateadd(
                'month', charge_term / 4 * 2, effective_start_month
            ) as effective_end_month,
            subscription_end_month,
            myb_subscription_end_month,
            sum(arr) as arr
        from agg_charge_term_greater_than_12
        where
            charge_term between 37 and 48
            and effective_end_month > '2022-01-01' {{ dbt_utils.group_by(n=18) }}
        union all
        select
            renewal_type,
            is_myb,
            is_myb_with_multi_subs,
            current_term,
            charge_term,
            charge_id,
            dim_crm_account_id,
            dim_billing_account_id,
            dim_crm_opportunity_id,
            dim_subscription_id,
            dim_product_detail_id,
            product_tier_name,
            product_delivery_type,
            subscription_name,
            effective_start_month,
            dateadd(
                'month', charge_term / 4 * 3, effective_start_month
            ) as effective_end_month,
            subscription_end_month,
            myb_subscription_end_month,
            sum(arr) as arr
        from agg_charge_term_greater_than_12
        where
            charge_term between 37 and 48
            and effective_end_month > '2022-01-01' {{ dbt_utils.group_by(n=18) }}
        order by 1

    ),
    sixty_mth_term as (  -- create records for the intermitent renewals for MYBs that are not in the Zuora data. The start and end months are in the agg_myb for MYBs.

        select
            renewal_type,
            is_myb,
            is_myb_with_multi_subs,
            current_term,
            charge_term,
            charge_id,
            dim_crm_account_id,
            dim_billing_account_id,
            dim_crm_opportunity_id,
            dim_subscription_id,
            dim_product_detail_id,
            product_tier_name,
            product_delivery_type,
            subscription_name,
            effective_start_month,
            dateadd(
                'month', charge_term / 5, effective_start_month
            ) as effective_end_month,
            subscription_end_month,
            myb_subscription_end_month,
            sum(arr) as arr
        from agg_charge_term_greater_than_12
        where
            charge_term between 49 and 60
            and effective_end_month > '2022-01-01' {{ dbt_utils.group_by(n=18) }}
        union all
        select
            renewal_type,
            is_myb,
            is_myb_with_multi_subs,
            current_term,
            charge_term,
            charge_id,
            dim_crm_account_id,
            dim_billing_account_id,
            dim_crm_opportunity_id,
            dim_subscription_id,
            dim_product_detail_id,
            product_tier_name,
            product_delivery_type,
            subscription_name,
            effective_start_month,
            dateadd(
                'month', charge_term / 5 * 2, effective_start_month
            ) as effective_end_month,
            subscription_end_month,
            myb_subscription_end_month,
            sum(arr) as arr
        from agg_charge_term_greater_than_12
        where
            charge_term between 49 and 60
            and effective_end_month > '2022-01-01' {{ dbt_utils.group_by(n=18) }}
        union all
        select
            renewal_type,
            is_myb,
            is_myb_with_multi_subs,
            current_term,
            charge_term,
            charge_id,
            dim_crm_account_id,
            dim_billing_account_id,
            dim_crm_opportunity_id,
            dim_subscription_id,
            dim_product_detail_id,
            product_tier_name,
            product_delivery_type,
            subscription_name,
            effective_start_month,
            dateadd(
                'month', charge_term / 5 * 3, effective_start_month
            ) as effective_end_month,
            subscription_end_month,
            myb_subscription_end_month,
            sum(arr) as arr
        from agg_charge_term_greater_than_12
        where
            charge_term between 49 and 60
            and effective_end_month > '2022-01-01' {{ dbt_utils.group_by(n=18) }}
        union all
        select
            renewal_type,
            is_myb,
            is_myb_with_multi_subs,
            current_term,
            charge_term,
            charge_id,
            dim_crm_account_id,
            dim_billing_account_id,
            dim_crm_opportunity_id,
            dim_subscription_id,
            dim_product_detail_id,
            product_tier_name,
            product_delivery_type,
            subscription_name,
            effective_start_month,
            dateadd(
                'month', charge_term / 5 * 4, effective_start_month
            ) as effective_end_month,
            subscription_end_month,
            myb_subscription_end_month,
            sum(arr) as arr
        from agg_charge_term_greater_than_12
        where
            charge_term between 49 and 60
            and effective_end_month > '2022-01-01' {{ dbt_utils.group_by(n=18) }}
        order by 1

    ),
    combined as (  -- union all of the charges

        select *
        from agg_charge_term_less_than_equal_12
        union all
        select *
        from agg_charge_term_greater_than_12
        union all
        select *
        from twenty_four_mth_term
        union all
        select *
        from thirty_six_mth_term
        union all
        select *
        from forty_eight_mth_term
        union all
        select *
        from sixty_mth_term

    ),
    renewal_report as (  -- create the renewal report for the applicable fiscal year.

        select
            dim_date.fiscal_year,
            dim_date.fiscal_quarter_name_fy,
            combined.effective_end_month,
            combined.effective_start_month,
            base.charge_id,
            base.dim_crm_account_id,
            base.dim_billing_account_id,
            base.dim_crm_opportunity_id as wip_dim_crm_opportunity_id,
            base.dim_subscription_id,
            base.dim_product_detail_id,
            base.subscription_name,
            base.subscription_start_month,
            base.subscription_end_month,
            base.myb_subscription_end_month,
            base.parent_crm_account_name,
            base.crm_account_name,
            base.parent_crm_account_sales_segment,
            base.product_tier_name,
            base.product_delivery_type,
            combined.renewal_type,
            case
                when
                    base.subscription_end_month between '2021-02-01' and '2022-01-01'
                    and base.is_myb_with_multi_subs = false
                then true
                else false
            end as is_atr,
            base.is_myb,
            base.is_myb_with_multi_subs,
            base.current_term as subscription_term,
            base.charge_term,
            base.arr,
            base.estimated_total_future_billings,
            base.estimated_fy22_future_billings
        from combined
        left join dim_date on combined.effective_end_month = dim_date.first_day_of_month
        left join base on combined.charge_id = base.charge_id
        where
            combined.effective_end_month between '2021-02-01' and '2022-01-01'
            and day_of_month = 1
        order by fiscal_quarter_name_fy

    )

    {{
        dbt_audit(
            cte_ref="renewal_report",
            created_by="@iweeks",
            updated_by="@iweeks",
            created_date="2021-03-15",
            updated_date="2021-08-05",
        )
    }}
