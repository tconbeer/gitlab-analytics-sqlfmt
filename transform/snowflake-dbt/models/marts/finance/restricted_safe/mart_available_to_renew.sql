{{ config({"materialized": "table", "transient": false}) }}

{% set renewal_fiscal_years = [
    "2019",
    "2020",
    "2021",
    "2022",
    "2023",
    "2024",
    "2025",
    "2026",
] %}

{{
    simple_cte(
        [
            ("dim_date", "dim_date"),
            ("dim_crm_account", "dim_crm_account"),
            ("dim_crm_user", "dim_crm_user"),
            ("dim_subscription", "dim_subscription"),
            ("dim_crm_opportunity", "dim_crm_opportunity"),
            ("fct_crm_opportunity", "fct_crm_opportunity"),
            ("dim_charge", "dim_charge"),
            ("fct_charge", "fct_charge"),
            ("dim_billing_account", "dim_billing_account"),
            ("dim_product_detail", "dim_product_detail"),
            ("dim_amendment", "dim_amendment"),
        ]
    )
}}

,
dim_subscription_source as (

    select
        dim_subscription.*,
        case
            when
                lead(term_start_month) over (
                    partition by subscription_name order by subscription_version
                ) = term_start_month
            then true
            else false
        end as is_dup_term
    from dim_subscription
    where
        -- data quality, last version is expired with no ARR in mart_arr. Should
        -- filter it out completely.
        dim_subscription_id not in (
            '2c92a0ff5e1dcf14015e3bb595f14eef',
            '2c92a0ff5e1dcf14015e3c191d4f7689',
            '2c92a007644967bc01645d54e7df49a8',
            '2c92a007644967bc01645d54e9b54a4b',
            '2c92a0ff5e1dcf1a015e3bf7a32475a5'
        )
        -- test subscription
        and subscription_name != 'Test- New Subscription'
        -- data quality, last term not entered with same pattern, sub_name =
        -- A-S00022101
        and dim_subscription_id != '2c92a00f7579c362017588a2de19174a'
        -- term dates do not align to the subscription term dates, sub_name =
        -- A-S00038937
        and dim_subscription_id != '2c92a01177472c5201774af57f834a43'
        -- data quality, last term not entered with same pattern that fits ATR logic.
        -- Edge cases that needs to be filtered out to get to the last term version
        -- that should count for this subscription.
        -- sub_name = A-S00011774
        and dim_subscription_id not in (
            '8a1298657dd7f81d017dde1bd9c03fa8',
            '8a128b317dd7e89a017ddd38a74d3037',
            '8a128b317dd7e89a017ddd38a6052ff0',
            '8a128b317dc30baa017dc41e5b0932e9',
            '8a128b317dc30baa017dc41e59dd32be',
            '8a128b317dc30baa017dc41e58b43295',
            '2c92a0fd7cc1ab13017cc843195f62fb',
            '2c92a0fd7cc1ab13017cc843186f62da',
            '2c92a0fd7cc1ab13017cc843178162b6',
            '2c92a0fd7cc1ab13017cc843164d6292'
        )

),
dim_subscription_int as (

    select
        dim_subscription_source.*,
        case
            when
                lead(term_end_month) over (
                    partition by subscription_name order by subscription_version
                ) = term_end_month
            then true
            when
                lead(term_end_month, 2) over (
                    partition by subscription_name order by subscription_version
                ) = term_end_month
            then true
            when
                lead(subscription_end_fiscal_year) over (
                    partition by subscription_name order by subscription_version
                ) = subscription_end_fiscal_year
            then true
            when
                lead(term_start_month) over (
                    partition by subscription_name order by subscription_version
                ) = term_start_month
            then true
            -- check for subsequent subscriptiptions that are backed out
            when
                lead(term_start_month) over (
                    partition by subscription_name order by subscription_version
                ) < term_start_month
            then true
            when
                lead(term_start_month, 2) over (
                    partition by subscription_name order by subscription_version
                ) < term_start_month
            then true
            when
                lead(term_start_month, 3) over (
                    partition by subscription_name order by subscription_version
                ) < term_start_month
            then true
            when
                lead(term_start_month, 4) over (
                    partition by subscription_name order by subscription_version
                ) < term_start_month
            then true
            when
                lead(term_start_month, 5) over (
                    partition by subscription_name order by subscription_version
                ) < term_start_month
            then true
            when
                lead(term_start_month, 6) over (
                    partition by subscription_name order by subscription_version
                ) < term_start_month
            then true
            when
                lead(term_start_month, 7) over (
                    partition by subscription_name order by subscription_version
                ) < term_start_month
            then true
            when
                lead(term_start_month, 8) over (
                    partition by subscription_name order by subscription_version
                ) < term_start_month
            then true
            when
                lead(term_start_month, 9) over (
                    partition by subscription_name order by subscription_version
                ) < term_start_month
            then true
            when
                lead(term_start_month, 10) over (
                    partition by subscription_name order by subscription_version
                ) < term_start_month
            then true
            when
                lead(term_start_month, 11) over (
                    partition by subscription_name order by subscription_version
                ) < term_start_month
            then true
            when
                lead(term_start_month, 12) over (
                    partition by subscription_name order by subscription_version
                ) < term_start_month
            then true
            when
                lead(term_start_month, 13) over (
                    partition by subscription_name order by subscription_version
                ) < term_start_month
            then true
            when
                lead(term_start_month, 14) over (
                    partition by subscription_name order by subscription_version
                ) < term_start_month
            then true
            else false
        end as exclude_from_term_sorting
    from dim_subscription_source
    where is_dup_term = false

),
base_subscriptions as (

    select
        dim_subscription_id,
        rank() over (
            partition by subscription_name, term_start_month
            order by subscription_version desc
        ) as last_term_version
    from dim_subscription_int
    where exclude_from_term_sorting = false

),
dim_subscription_last_term as (

    select dim_subscription.*
    from dim_subscription
    inner join
        base_subscriptions
        on dim_subscription.dim_subscription_id = base_subscriptions.dim_subscription_id
    where last_term_version = 1

),
mart_charge_base as (

    select
        -- Surrogate Key
        dim_charge.dim_charge_id as dim_charge_id,

        -- Natural Key
        dim_charge.subscription_name as subscription_name,
        dim_charge.subscription_version as subscription_version,
        dim_charge.rate_plan_charge_number as rate_plan_charge_number,
        dim_charge.rate_plan_charge_version as rate_plan_charge_version,
        dim_charge.rate_plan_charge_segment as rate_plan_charge_segment,

        -- Charge Information
        dim_charge.rate_plan_name as rate_plan_name,
        dim_charge.rate_plan_charge_name as rate_plan_charge_name,
        dim_charge.charge_type as charge_type,
        dim_charge.is_paid_in_full as is_paid_in_full,
        dim_charge.is_last_segment as is_last_segment,
        dim_charge.is_included_in_arr_calc as is_included_in_arr_calc,
        dim_charge.effective_start_date as effective_start_date,
        dim_charge.effective_end_date as effective_end_date,
        dim_charge.effective_start_month as effective_start_month,
        dim_charge.effective_end_month as effective_end_month,
        dim_charge.charge_created_date as charge_created_date,
        dim_charge.charge_updated_date as charge_updated_date,

        -- Subscription Information
        dim_subscription.dim_subscription_id as dim_subscription_id,
        dim_subscription.created_by_id as subscription_created_by_id,
        dim_subscription.updated_by_id as subscription_updated_by_id,
        dim_subscription.subscription_start_date as subscription_start_date,
        dim_subscription.subscription_end_date as subscription_end_date,
        dim_subscription.subscription_start_month as subscription_start_month,
        dim_subscription.subscription_end_month as subscription_end_month,
        dim_subscription.subscription_end_fiscal_year as subscription_end_fiscal_year,
        dim_subscription.subscription_created_date as subscription_created_date,
        dim_subscription.subscription_updated_date as subscription_updated_date,
        dim_subscription.second_active_renewal_month as second_active_renewal_month,
        dim_subscription.term_start_date,
        dim_subscription.term_end_date,
        dim_subscription.term_start_month,
        dim_subscription.term_end_month,
        dim_subscription.subscription_status as subscription_status,
        dim_subscription.subscription_sales_type as subscription_sales_type,
        dim_subscription.subscription_name_slugify as subscription_name_slugify,
        dim_subscription.oldest_subscription_in_cohort as oldest_subscription_in_cohort,
        dim_subscription.subscription_lineage as subscription_lineage,
        dim_subscription.auto_renew_native_hist,
        dim_subscription.auto_renew_customerdot_hist,
        dim_subscription.turn_on_cloud_licensing,
        dim_subscription.turn_on_operational_metrics,
        dim_subscription.contract_operational_metrics,
        dim_subscription.contract_auto_renewal,
        dim_subscription.turn_on_auto_renewal,
        dim_subscription.contract_seat_reconciliation,
        dim_subscription.turn_on_seat_reconciliation,

        -- billing account info
        dim_billing_account.dim_billing_account_id as dim_billing_account_id,
        dim_billing_account.sold_to_country as sold_to_country,
        dim_billing_account.billing_account_name as billing_account_name,
        dim_billing_account.billing_account_number as billing_account_number,
        dim_billing_account.ssp_channel as ssp_channel,
        dim_billing_account.po_required as po_required,

        -- crm account info
        dim_crm_user.dim_crm_user_id as dim_crm_user_id,
        dim_crm_user.crm_user_sales_segment as crm_user_sales_segment,
        dim_crm_account.dim_crm_account_id as dim_crm_account_id,
        dim_crm_account.crm_account_name as crm_account_name,
        dim_crm_account.dim_parent_crm_account_id as dim_parent_crm_account_id,
        dim_crm_account.parent_crm_account_name as parent_crm_account_name,
        dim_crm_account.parent_crm_account_billing_country
        as parent_crm_account_billing_country,
        dim_crm_account.parent_crm_account_sales_segment
        as parent_crm_account_sales_segment,
        dim_crm_account.parent_crm_account_industry as parent_crm_account_industry,
        dim_crm_account.parent_crm_account_owner_team as parent_crm_account_owner_team,
        dim_crm_account.parent_crm_account_sales_territory
        as parent_crm_account_sales_territory,
        dim_crm_account.parent_crm_account_tsp_region as parent_crm_account_tsp_region,
        dim_crm_account.parent_crm_account_tsp_sub_region
        as parent_crm_account_tsp_sub_region,
        dim_crm_account.parent_crm_account_tsp_area as parent_crm_account_tsp_area,
        dim_crm_account.crm_account_tsp_region as crm_account_tsp_region,
        dim_crm_account.crm_account_tsp_sub_region as crm_account_tsp_sub_region,
        dim_crm_account.crm_account_tsp_area as crm_account_tsp_area,
        dim_crm_account.health_score as health_score,
        dim_crm_account.health_score_color as health_score_color,
        dim_crm_account.health_number as health_number,
        dim_crm_account.is_jihu_account as is_jihu_account,

        -- Cohort Information
        dim_subscription.subscription_cohort_month as subscription_cohort_month,
        dim_subscription.subscription_cohort_quarter as subscription_cohort_quarter,
        min(dim_subscription.subscription_cohort_month) over (
            partition by dim_billing_account.dim_billing_account_id
        ) as billing_account_cohort_month,
        min(dim_subscription.subscription_cohort_quarter) over (
            partition by dim_billing_account.dim_billing_account_id
        ) as billing_account_cohort_quarter,
        min(dim_subscription.subscription_cohort_month) over (
            partition by dim_crm_account.dim_crm_account_id
        ) as crm_account_cohort_month,
        min(dim_subscription.subscription_cohort_quarter) over (
            partition by dim_crm_account.dim_crm_account_id
        ) as crm_account_cohort_quarter,
        min(dim_subscription.subscription_cohort_month) over (
            partition by dim_crm_account.dim_parent_crm_account_id
        ) as parent_account_cohort_month,
        min(dim_subscription.subscription_cohort_quarter) over (
            partition by dim_crm_account.dim_parent_crm_account_id
        ) as parent_account_cohort_quarter,

        -- product info
        dim_product_detail.dim_product_detail_id,
        dim_product_detail.product_tier_name as product_tier_name,
        dim_product_detail.product_delivery_type as product_delivery_type,
        dim_product_detail.service_type as service_type,
        dim_product_detail.product_rate_plan_name as product_rate_plan_name,

        -- Amendment Information
        dim_subscription.dim_amendment_id_subscription,
        fct_charge.dim_amendment_id_charge,
        dim_amendment_subscription.effective_date
        as subscription_amendment_effective_date,
        case
            when dim_charge.subscription_version = 1
            then 'NewSubscription'
            else dim_amendment_subscription.amendment_type
        end as subscription_amendment_type,
        dim_amendment_subscription.amendment_name as subscription_amendment_name,
        case
            when dim_charge.subscription_version = 1
            then 'NewSubscription'
            else dim_amendment_charge.amendment_type
        end as charge_amendment_type,

        -- ARR Analysis Framework
        dim_charge.type_of_arr_change,

        -- Additive Fields
        fct_charge.mrr,
        fct_charge.previous_mrr,
        fct_charge.delta_mrr,
        fct_charge.arr,
        fct_charge.previous_arr,
        fct_charge.delta_arr,
        fct_charge.quantity,
        fct_charge.previous_quantity,
        fct_charge.delta_quantity,
        fct_charge.delta_tcv,
        fct_charge.estimated_total_future_billings
    from fct_charge
    inner join dim_charge on fct_charge.dim_charge_id = dim_charge.dim_charge_id
    inner join
        dim_subscription
        on fct_charge.dim_subscription_id = dim_subscription.dim_subscription_id
    inner join
        dim_product_detail
        on fct_charge.dim_product_detail_id = dim_product_detail.dim_product_detail_id
    inner join
        dim_billing_account
        on fct_charge.dim_billing_account_id
        = dim_billing_account.dim_billing_account_id
    left join
        dim_crm_account
        on dim_crm_account.dim_crm_account_id = dim_billing_account.dim_crm_account_id
    left join
        dim_crm_user on dim_crm_account.dim_crm_user_id = dim_crm_user.dim_crm_user_id
    left join
        dim_amendment as dim_amendment_subscription
        on dim_subscription.dim_amendment_id_subscription
        = dim_amendment_subscription.dim_amendment_id
    left join
        dim_amendment as dim_amendment_charge
        on fct_charge.dim_amendment_id_charge = dim_amendment_charge.dim_amendment_id
    where dim_crm_account.is_jihu_account != 'TRUE'

),
mart_charge as (

    select mart_charge_base.*
    from mart_charge_base
    inner join
        dim_subscription_last_term
        on mart_charge_base.dim_subscription_id
        = dim_subscription_last_term.dim_subscription_id
    where
        is_included_in_arr_calc = 'TRUE'
        and mart_charge_base.term_end_month = mart_charge_base.effective_end_month
        and arr != 0

{% for renewal_fiscal_year in renewal_fiscal_years -%}
),
renewal_subscriptions_{{ renewal_fiscal_year }} as (

    select distinct
        sub_1.subscription_name,
        sub_1.zuora_renewal_subscription_name,
        date_trunc('month', sub_2.subscription_end_date) as subscription_end_month,
        rank() over (
            partition by sub_1.subscription_name
            order by sub_2.subscription_end_date desc
        ) as rank
    from dim_subscription_last_term sub_1
    inner join
        dim_subscription_last_term sub_2
        on sub_1.zuora_renewal_subscription_name = sub_2.subscription_name
        and date_trunc('month', sub_2.subscription_end_date) > concat(
            '{{renewal_fiscal_year}}', '-01-01'
        )
    where sub_1.zuora_renewal_subscription_name != ''
    qualify rank = 1

),  -- get the base data set of recurring charges.
base_{{ renewal_fiscal_year }} as (

    select
        mart_charge.dim_charge_id,
        mart_charge.dim_crm_account_id,
        mart_charge.dim_billing_account_id,
        mart_charge.dim_subscription_id,
        mart_charge.dim_product_detail_id,
        mart_charge.parent_crm_account_name,
        mart_charge.crm_account_name,
        mart_charge.parent_crm_account_sales_segment,
        dim_crm_user.dim_crm_user_id,
        dim_crm_user.user_name,
        dim_crm_user.user_role_id,
        dim_crm_user.crm_user_sales_segment,
        dim_crm_user.crm_user_geo,
        dim_crm_user.crm_user_region,
        dim_crm_user.crm_user_area,
        mart_charge.product_tier_name,
        mart_charge.product_delivery_type,
        mart_charge.subscription_name,
        dim_subscription_last_term.zuora_renewal_subscription_name,
        dim_subscription_last_term.current_term,
        case
            when dim_subscription_last_term.current_term >= 24
            then true
            when
                dim_subscription_last_term.subscription_name in (
                    select distinct subscription_name
                    from renewal_subscriptions_{{ renewal_fiscal_year }}
                )
            then true
            else false
        end as is_multi_year_booking,
        case
            when
                dim_subscription_last_term.subscription_name in (
                    select distinct subscription_name
                    from renewal_subscriptions_{{ renewal_fiscal_year }}
                )
            then true
            else false
        end as is_multi_year_booking_with_multi_subs,
        mart_charge.is_paid_in_full,
        mart_charge.estimated_total_future_billings,
        mart_charge.effective_start_month,
        mart_charge.effective_end_month,
        mart_charge.subscription_start_month,
        mart_charge.subscription_end_month,
        mart_charge.term_start_month,
        mart_charge.term_end_month,
        dateadd('month', -1, mart_charge.term_end_month) as last_paid_month_in_term,
        renewal_subscriptions_{{ renewal_fiscal_year }}.subscription_end_month
        as multi_year_booking_subscription_end_month,
        datediff(
            month, mart_charge.effective_start_month, mart_charge.effective_end_month
        ) as charge_term,
        mart_charge.arr
    from mart_charge
    left join
        dim_subscription_last_term
        on mart_charge.dim_subscription_id
        = dim_subscription_last_term.dim_subscription_id
    left join
        dim_crm_account
        on mart_charge.dim_crm_account_id = dim_crm_account.dim_crm_account_id
    left join
        dim_crm_user on dim_crm_account.dim_crm_user_id = dim_crm_user.dim_crm_user_id
    left join
        renewal_subscriptions_{{ renewal_fiscal_year }}
        on mart_charge.subscription_name
        = renewal_subscriptions_{{ renewal_fiscal_year }}.subscription_name
    where
        mart_charge.term_start_month <= concat(
            '{{renewal_fiscal_year}}' -1, '-01-01'
        ) and mart_charge.term_end_month > concat(
            '{{renewal_fiscal_year}}' -1, '-01-01'
        )

-- get the starting and ending month ARR for charges with current terms <= 12 months.
-- These terms do not need additional logic.
),
agg_charge_term_less_than_equal_12_{{ renewal_fiscal_year }} as (

    select
        case
            when is_multi_year_booking = true then 'MYB' else 'Non-MYB'
        end as renewal_type,
        is_multi_year_booking,
        is_multi_year_booking_with_multi_subs,
        current_term,
        -- charge_term,
        dim_charge_id,
        dim_crm_account_id,
        dim_billing_account_id,
        dim_subscription_id,
        dim_crm_user_id,
        user_name,
        user_role_id,
        crm_user_sales_segment,
        crm_user_geo,
        crm_user_region,
        crm_user_area,
        dim_product_detail_id,
        product_tier_name,
        product_delivery_type,
        subscription_name,
        term_start_month,
        term_end_month,
        subscription_end_month,
        sum(arr) as arr
    from base_{{ renewal_fiscal_year }}
    where current_term <= 12 {{ dbt_utils.group_by(n=22) }}

-- get the starting and ending month ARR for terms > 12 months. These terms need
-- additional logic.
),
agg_charge_term_greater_than_12_{{ renewal_fiscal_year }} as (

    select
        case
            when is_multi_year_booking = true then 'MYB' else 'Non-MYB'
        end as renewal_type,
        is_multi_year_booking,
        is_multi_year_booking_with_multi_subs,
        -- current_term,
        -- the below odd term charges do not behave well in the multi-year bookings
        -- logic and end up with duplicate renewals in the fiscal year. This CASE
        -- statement smooths out the charges so they only have one renewal entry in
        -- the fiscal year.
        case
            when current_term = 25
            then 24
            when current_term = 26
            then 24
            when current_term = 27
            then 36
            when current_term = 28
            then 36
            when current_term = 29
            then 36
            when current_term = 30
            then 36
            when current_term = 31
            then 36
            when current_term = 32
            then 36
            when current_term = 35
            then 36
            when current_term = 37
            then 36
            when current_term = 38
            then 36
            when current_term = 41
            then 36
            when current_term = 42
            then 48
            when current_term = 49
            then 48
            when current_term = 57
            then 60
            else current_term
        end as current_term,
        dim_charge_id,
        dim_crm_account_id,
        dim_billing_account_id,
        dim_subscription_id,
        dim_crm_user_id,
        user_name,
        user_role_id,
        crm_user_sales_segment,
        crm_user_geo,
        crm_user_region,
        crm_user_area,
        dim_product_detail_id,
        product_tier_name,
        product_delivery_type,
        subscription_name,
        term_start_month,
        term_end_month,
        subscription_end_month,
        sum(arr) as arr
    from base_{{ renewal_fiscal_year }}
    where current_term > 12 {{ dbt_utils.group_by(n=22) }}

-- create records for the intermitent renewals for multi-year charges that are not in
-- the Zuora data. The start and end months are in the agg_myb for multi-year
-- bookings.
),
twenty_four_mth_term_{{ renewal_fiscal_year }} as (

    select
        renewal_type,
        is_multi_year_booking,
        is_multi_year_booking_with_multi_subs,
        current_term,
        dim_charge_id,
        dim_crm_account_id,
        dim_billing_account_id,
        dim_subscription_id,
        dim_crm_user_id,
        user_name,
        user_role_id,
        crm_user_sales_segment,
        crm_user_geo,
        crm_user_region,
        crm_user_area,
        dim_product_detail_id,
        product_tier_name,
        product_delivery_type,
        subscription_name,
        term_start_month,
        dateadd('month', current_term / 2, term_start_month) as term_end_month,
        subscription_end_month,
        sum(arr) as arr
    from agg_charge_term_greater_than_12_{{ renewal_fiscal_year }}
    where
        current_term between 13 and 24 and term_end_month > concat(
            '{{renewal_fiscal_year}}', '-01-01'
        )
        {{ dbt_utils.group_by(n=22) }}

-- create records for the intermitent renewals for multi-year bookings that are not in
-- the Zuora data. The start and end months are in the agg_myb for multi-year
-- bookings.
),
thirty_six_mth_term_{{ renewal_fiscal_year }} as (

    select
        renewal_type,
        is_multi_year_booking,
        is_multi_year_booking_with_multi_subs,
        current_term,
        dim_charge_id,
        dim_crm_account_id,
        dim_billing_account_id,
        dim_subscription_id,
        dim_crm_user_id,
        user_name,
        user_role_id,
        crm_user_sales_segment,
        crm_user_geo,
        crm_user_region,
        crm_user_area,
        dim_product_detail_id,
        product_tier_name,
        product_delivery_type,
        subscription_name,
        term_start_month,
        dateadd('month', current_term / 3, term_start_month) as term_end_month,
        subscription_end_month,
        sum(arr) as arr
    from agg_charge_term_greater_than_12_{{ renewal_fiscal_year }}
    where
        current_term between 25 and 36 and term_end_month > concat(
            '{{renewal_fiscal_year}}', '-01-01'
        )
        {{ dbt_utils.group_by(n=22) }}

    UNION ALL

    select
        renewal_type,
        is_multi_year_booking,
        is_multi_year_booking_with_multi_subs,
        current_term,
        dim_charge_id,
        dim_crm_account_id,
        dim_billing_account_id,
        dim_subscription_id,
        dim_crm_user_id,
        user_name,
        user_role_id,
        crm_user_sales_segment,
        crm_user_geo,
        crm_user_region,
        crm_user_area,
        dim_product_detail_id,
        product_tier_name,
        product_delivery_type,
        subscription_name,
        term_start_month,
        dateadd('month', current_term / 3 * 2, term_start_month) as term_end_month,
        subscription_end_month,
        sum(arr) as arr
    from agg_charge_term_greater_than_12_{{ renewal_fiscal_year }}
    where
        current_term between 25 and 36 and term_end_month > concat(
            '{{renewal_fiscal_year}}', '-01-01'
        )
        {{ dbt_utils.group_by(n=22) }}
    order by 1

-- create records for the intermitent renewals for multi-year bookings that are not in
-- the Zuora data. The start and end months are in the agg_MYB for multi-year
-- bookings.
),
forty_eight_mth_term_{{ renewal_fiscal_year }} as (

    select
        renewal_type,
        is_multi_year_booking,
        is_multi_year_booking_with_multi_subs,
        current_term,
        dim_charge_id,
        dim_crm_account_id,
        dim_billing_account_id,
        dim_subscription_id,
        dim_crm_user_id,
        user_name,
        user_role_id,
        crm_user_sales_segment,
        crm_user_geo,
        crm_user_region,
        crm_user_area,
        dim_product_detail_id,
        product_tier_name,
        product_delivery_type,
        subscription_name,
        term_start_month,
        dateadd('month', current_term / 4, term_start_month) as term_end_month,
        subscription_end_month,
        sum(arr) as arr
    from agg_charge_term_greater_than_12_{{ renewal_fiscal_year }}
    where
        current_term between 37 and 48 and term_end_month > concat(
            '{{renewal_fiscal_year}}', '-01-01'
        )
        {{ dbt_utils.group_by(n=22) }}

    UNION ALL

    select
        renewal_type,
        is_multi_year_booking,
        is_multi_year_booking_with_multi_subs,
        current_term,
        dim_charge_id,
        dim_crm_account_id,
        dim_billing_account_id,
        dim_subscription_id,
        dim_crm_user_id,
        user_name,
        user_role_id,
        crm_user_sales_segment,
        crm_user_geo,
        crm_user_region,
        crm_user_area,
        dim_product_detail_id,
        product_tier_name,
        product_delivery_type,
        subscription_name,
        term_start_month,
        dateadd('month', current_term / 4 * 2, term_start_month) as term_end_month,
        subscription_end_month,
        sum(arr) as arr
    from agg_charge_term_greater_than_12_{{ renewal_fiscal_year }}
    where
        current_term between 37 and 48 and term_end_month > concat(
            '{{renewal_fiscal_year}}', '-01-01'
        )
        {{ dbt_utils.group_by(n=22) }}

    UNION ALL

    select
        renewal_type,
        is_multi_year_booking,
        is_multi_year_booking_with_multi_subs,
        current_term,
        dim_charge_id,
        dim_crm_account_id,
        dim_billing_account_id,
        dim_subscription_id,
        dim_crm_user_id,
        user_name,
        user_role_id,
        crm_user_sales_segment,
        crm_user_geo,
        crm_user_region,
        crm_user_area,
        dim_product_detail_id,
        product_tier_name,
        product_delivery_type,
        subscription_name,
        term_start_month,
        dateadd('month', current_term / 4 * 3, term_start_month) as term_end_month,
        subscription_end_month,
        sum(arr) as arr
    from agg_charge_term_greater_than_12_{{ renewal_fiscal_year }}
    where
        current_term between 37 and 48 and term_end_month > concat(
            '{{renewal_fiscal_year}}', '-01-01'
        )
        {{ dbt_utils.group_by(n=22) }}
    order by 1

-- create records for the intermitent renewals for multi-year bookings that are not in
-- the Zuora data. The start and end months are in the agg_MYB for multi-year
-- bookings.
),
sixty_mth_term_{{ renewal_fiscal_year }} as (

    select
        renewal_type,
        is_multi_year_booking,
        is_multi_year_booking_with_multi_subs,
        current_term,
        dim_charge_id,
        dim_crm_account_id,
        dim_billing_account_id,
        dim_subscription_id,
        dim_crm_user_id,
        user_name,
        user_role_id,
        crm_user_sales_segment,
        crm_user_geo,
        crm_user_region,
        crm_user_area,
        dim_product_detail_id,
        product_tier_name,
        product_delivery_type,
        subscription_name,
        term_start_month,
        dateadd('month', current_term / 5, term_start_month) as term_end_month,
        subscription_end_month,
        sum(arr) as arr
    from agg_charge_term_greater_than_12_{{ renewal_fiscal_year }}
    where
        current_term between 49 and 60 and term_end_month > concat(
            '{{renewal_fiscal_year}}', '-01-01'
        )
        {{ dbt_utils.group_by(n=22) }}

    UNION ALL

    select
        renewal_type,
        is_multi_year_booking,
        is_multi_year_booking_with_multi_subs,
        current_term,
        dim_charge_id,
        dim_crm_account_id,
        dim_billing_account_id,
        dim_subscription_id,
        dim_crm_user_id,
        user_name,
        user_role_id,
        crm_user_sales_segment,
        crm_user_geo,
        crm_user_region,
        crm_user_area,
        dim_product_detail_id,
        product_tier_name,
        product_delivery_type,
        subscription_name,
        term_start_month,
        dateadd('month', current_term / 5 * 2, term_start_month) as term_end_month,
        subscription_end_month,
        sum(arr) as arr
    from agg_charge_term_greater_than_12_{{ renewal_fiscal_year }}
    where
        current_term between 49 and 60 and term_end_month > concat(
            '{{renewal_fiscal_year}}', '-01-01'
        )
        {{ dbt_utils.group_by(n=22) }}

    UNION ALL

    select
        renewal_type,
        is_multi_year_booking,
        is_multi_year_booking_with_multi_subs,
        current_term,
        dim_charge_id,
        dim_crm_account_id,
        dim_billing_account_id,
        dim_subscription_id,
        dim_crm_user_id,
        user_name,
        user_role_id,
        crm_user_sales_segment,
        crm_user_geo,
        crm_user_region,
        crm_user_area,
        dim_product_detail_id,
        product_tier_name,
        product_delivery_type,
        subscription_name,
        term_start_month,
        dateadd('month', current_term / 5 * 3, term_start_month) as term_end_month,
        subscription_end_month,
        sum(arr) as arr
    from agg_charge_term_greater_than_12_{{ renewal_fiscal_year }}
    where
        current_term between 49 and 60 and term_end_month > concat(
            '{{renewal_fiscal_year}}', '-01-01'
        )
        {{ dbt_utils.group_by(n=22) }}

    UNION ALL

    select
        renewal_type,
        is_multi_year_booking,
        is_multi_year_booking_with_multi_subs,
        current_term,
        dim_charge_id,
        dim_crm_account_id,
        dim_billing_account_id,
        dim_subscription_id,
        dim_crm_user_id,
        user_name,
        user_role_id,
        crm_user_sales_segment,
        crm_user_geo,
        crm_user_region,
        crm_user_area,
        dim_product_detail_id,
        product_tier_name,
        product_delivery_type,
        subscription_name,
        term_start_month,
        dateadd('month', current_term / 5 * 4, term_start_month) as term_end_month,
        subscription_end_month,
        sum(arr) as arr
    from agg_charge_term_greater_than_12_{{ renewal_fiscal_year }}
    where
        current_term between 49 and 60 and term_end_month > concat(
            '{{renewal_fiscal_year}}', '-01-01'
        )
        {{ dbt_utils.group_by(n=22) }}
    order by 1

),  -- union all of the charges
combined_{{ renewal_fiscal_year }} as (

    select *
    from agg_charge_term_less_than_equal_12_{{ renewal_fiscal_year }}

    UNION ALL

    select *
    from agg_charge_term_greater_than_12_{{ renewal_fiscal_year }}

    UNION ALL

    select *
    from twenty_four_mth_term_{{ renewal_fiscal_year }}

    UNION ALL

    select *
    from thirty_six_mth_term_{{ renewal_fiscal_year }}

    UNION ALL

    select *
    from forty_eight_mth_term_{{ renewal_fiscal_year }}

    UNION ALL

    select *
    from sixty_mth_term_{{ renewal_fiscal_year }}

),
opportunity_term_group as (

    select
        dim_subscription.dim_subscription_id,
        dim_crm_opportunity.dim_crm_opportunity_id,
        case
            when close_date is null
            then '1951-01-01'
            else date_trunc('month', close_date)
        end as close_month,
        case
            when dim_crm_opportunity.opportunity_term = 0
            then '0 Years'
            when dim_crm_opportunity.opportunity_term <= 12
            then '1 Year'
            when
                dim_crm_opportunity.opportunity_term > 12
                and dim_crm_opportunity.opportunity_term <= 24
            then '2 Years'
            when
                dim_crm_opportunity.opportunity_term > 24
                and dim_crm_opportunity.opportunity_term <= 36
            then '3 Years'
            when dim_crm_opportunity.opportunity_term > 36
            then '4 Years+'
            when dim_crm_opportunity.opportunity_term is null
            then 'No Opportunity Term'
        end as opportunity_term_group
    from dim_subscription
    left join
        dim_crm_opportunity
        on dim_subscription.dim_crm_opportunity_id
        = dim_crm_opportunity.dim_crm_opportunity_id
    left join
        fct_crm_opportunity
        on dim_subscription.dim_crm_opportunity_id
        = fct_crm_opportunity.dim_crm_opportunity_id

),  -- create the renewal report for the applicable fiscal year.
renewal_report_{{ renewal_fiscal_year }} as (

    select
        concat(
            dim_date.fiscal_quarter_name_fy,
            base_{{ renewal_fiscal_year }}.term_end_month,
            base_{{ renewal_fiscal_year }}.dim_charge_id
        ) as concat_primary_key,
        {{ dbt_utils.surrogate_key(["concat_primary_key"]) }} as primary_key,
        dim_date.fiscal_year as fiscal_year,
        dim_date.fiscal_quarter_name_fy as fiscal_quarter_name_fy,
        opportunity_term_group.close_month as close_month,
        base_{{ renewal_fiscal_year }}.dim_charge_id as dim_charge_id,
        opportunity_term_group.dim_crm_opportunity_id as dim_crm_opportunity_id,
        base_{{ renewal_fiscal_year }}.dim_crm_account_id as dim_crm_account_id,
        base_{{ renewal_fiscal_year }}.dim_billing_account_id as dim_billing_account_id,
        base_{{ renewal_fiscal_year }}.dim_subscription_id as dim_subscription_id,
        base_{{ renewal_fiscal_year }}.dim_product_detail_id as dim_product_detail_id,
        base_{{ renewal_fiscal_year }}.subscription_name as subscription_name,
        base_{{ renewal_fiscal_year }}.subscription_start_month
        as subscription_start_month,
        base_{{ renewal_fiscal_year }}.subscription_end_month as subscription_end_month,
        base_{{ renewal_fiscal_year }}.term_start_month as term_start_month,
        base_{{ renewal_fiscal_year }}.term_end_month as renewal_month,
        combined_{{ renewal_fiscal_year }}.term_end_month as bookings_term_end_month,
        base_{{ renewal_fiscal_year }}.multi_year_booking_subscription_end_month
        as multi_year_booking_subscription_end_month,
        base_{{ renewal_fiscal_year }}.last_paid_month_in_term
        as last_paid_month_in_term,
        base_{{ renewal_fiscal_year }}.current_term as current_term,
        renewal_subscriptions_{{ renewal_fiscal_year }}.zuora_renewal_subscription_name
        as zuora_renewal_subscription_name,
        renewal_subscriptions_{{ renewal_fiscal_year }}.subscription_end_month
        as renewal_subscription_end_month,
        base_{{ renewal_fiscal_year }}.parent_crm_account_name
        as parent_crm_account_name,
        base_{{ renewal_fiscal_year }}.crm_account_name as crm_account_name,
        base_{{ renewal_fiscal_year }}.parent_crm_account_sales_segment
        as parent_crm_account_sales_segment,
        base_{{ renewal_fiscal_year }}.dim_crm_user_id as dim_crm_user_id,
        base_{{ renewal_fiscal_year }}.user_name as user_name,
        base_{{ renewal_fiscal_year }}.user_role_id as user_role_id,
        base_{{ renewal_fiscal_year }}.crm_user_sales_segment as crm_user_sales_segment,
        base_{{ renewal_fiscal_year }}.crm_user_geo as crm_user_geo,
        base_{{ renewal_fiscal_year }}.crm_user_region as crm_user_region,
        base_{{ renewal_fiscal_year }}.crm_user_area as crm_user_area,
        base_{{ renewal_fiscal_year }}.product_tier_name as product_tier_name,
        base_{{ renewal_fiscal_year }}.product_delivery_type as product_delivery_type,
        combined_{{ renewal_fiscal_year }}.renewal_type as renewal_type,
        base_{{ renewal_fiscal_year }}.is_multi_year_booking as is_multi_year_booking,
        base_{{ renewal_fiscal_year }}.is_multi_year_booking_with_multi_subs
        as is_multi_year_booking_with_multi_subs,
        base_{{ renewal_fiscal_year }}.current_term as subscription_term,
        base_{{ renewal_fiscal_year }}.estimated_total_future_billings
        as estimated_total_future_billings,
        case
            when
                base_{{ renewal_fiscal_year }}.term_end_month between dateadd(
                    'month', 1, concat('{{renewal_fiscal_year}}' -1, '-01-01')
                ) and concat('{{renewal_fiscal_year}}', '-01-01')
                and base_{{ renewal_fiscal_year }}.is_multi_year_booking_with_multi_subs
                = false
            then true
            else false
        end as is_available_to_renew,
        case
            when opportunity_term_group.opportunity_term_group is null
            then 'No Opportunity Term'
            else opportunity_term_group.opportunity_term_group
        end as opportunity_term_group,
        base_{{ renewal_fiscal_year }}.arr as arr
    from combined_{{ renewal_fiscal_year }}
    left join
        dim_date
        on combined_{{ renewal_fiscal_year }}.term_end_month
        = dim_date.first_day_of_month
    left join
        base_{{ renewal_fiscal_year }}
        on combined_{{ renewal_fiscal_year }}.dim_charge_id
        = base_{{ renewal_fiscal_year }}.dim_charge_id
    left join
        renewal_subscriptions_{{ renewal_fiscal_year }}
        on base_{{ renewal_fiscal_year }}.subscription_name
        = renewal_subscriptions_{{ renewal_fiscal_year }}.subscription_name
    left join
        opportunity_term_group
        on base_{{ renewal_fiscal_year }}.dim_subscription_id
        = opportunity_term_group.dim_subscription_id
    where
        combined_{{ renewal_fiscal_year }}.term_end_month between dateadd(
            'month', 1, concat('{{renewal_fiscal_year}}' -1, '-01-01')
        ) and concat('{{renewal_fiscal_year}}', '-01-01') and day_of_month = 1
    order by fiscal_quarter_name_fy

{% endfor -%}
),
unioned as (

    {% for renewal_fiscal_year in renewal_fiscal_years -%}

    select
        primary_key,
        fiscal_year,
        fiscal_quarter_name_fy,
        close_month,
        dim_charge_id,
        dim_crm_opportunity_id,
        dim_crm_account_id,
        dim_billing_account_id,
        dim_subscription_id,
        dim_product_detail_id,
        subscription_name,
        subscription_start_month,
        subscription_end_month,
        term_start_month,
        renewal_month,
        bookings_term_end_month,
        multi_year_booking_subscription_end_month,
        last_paid_month_in_term,
        current_term,
        zuora_renewal_subscription_name,
        renewal_subscription_end_month,
        parent_crm_account_name,
        crm_account_name,
        parent_crm_account_sales_segment,
        dim_crm_user_id,
        user_name,
        user_role_id,
        crm_user_sales_segment,
        crm_user_geo,
        crm_user_region,
        crm_user_area,
        product_tier_name,
        product_delivery_type,
        renewal_type,
        is_multi_year_booking,
        is_multi_year_booking_with_multi_subs,
        subscription_term,
        estimated_total_future_billings,
        is_available_to_renew,
        opportunity_term_group,
        arr
    from renewal_report_{{ renewal_fiscal_year }}
    {%- if not loop.last %} UNION ALL {%- endif %}

    {% endfor -%}

)

{{
    dbt_audit(
        cte_ref="unioned",
        created_by="@michellecooper",
        updated_by="@iweeks",
        created_date="2021-12-06",
        updated_date="2022-01-21",
    )
}}
