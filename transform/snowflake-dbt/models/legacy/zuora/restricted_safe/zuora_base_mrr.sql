with
    zuora_accts as (select * from {{ ref("zuora_account") }}),
    zuora_subscriptions_xf as (select * from {{ ref("zuora_subscription_xf") }}),
    zuora_subscription_periods as (

        select * from {{ ref("zuora_subscription_periods") }}

    ),
    zuora_rp as (select * from {{ ref("zuora_rate_plan") }}),
    zuora_contact as (select * from {{ ref("zuora_contact") }}),
    zuora_rpc as (select * from {{ ref("zuora_rate_plan_charge") }}),
    original_rate_plan_id as (

        select distinct
            zuora_rpc.original_id,
            first_value(subscription_version_term_start_date) over (
                partition by original_id order by periods.version
            ) as subscription_version_term_start_date,
            first_value(subscription_version_term_end_date) over (
                partition by original_id order by periods.version
            ) as subscription_version_term_end_date

        from zuora_subscription_periods as periods
        inner join zuora_rp on periods.subscription_id = zuora_rp.subscription_id
        inner join zuora_rpc on zuora_rp.rate_plan_id = zuora_rpc.rate_plan_id

    ),
    base_mrr as (

        select
            zuora_rpc.rate_plan_charge_id,


            -- account info
            zuora_accts.account_name,
            zuora_accts.account_number,
            zuora_contact.country,
            zuora_accts.currency,

            -- subscription info
            zuora_subscriptions_xf.subscription_name,
            zuora_subscriptions_xf.subscription_name_slugify,
            zuora_subscriptions_xf.subscription_start_date,

            -- subscription_lineage info
            zuora_subscriptions_xf.exclude_from_renewal_report,
            zuora_subscriptions_xf.lineage,
            zuora_subscriptions_xf.oldest_subscription_in_cohort,
            zuora_subscriptions_xf.subscription_status,

            -- rate_plan info
            zuora_rp.delivery,
            zuora_rp.product_category,
            zuora_rp.rate_plan_name,

            -- 
            zuora_rpc.mrr,
            zuora_rpc.rate_plan_charge_name,
            zuora_rpc.rate_plan_charge_number,
            zuora_rpc.tcv,
            zuora_rpc.unit_of_measure,
            zuora_rpc.quantity,

            date_trunc(
                'month', zuora_subscriptions_xf.subscription_start_date::date
            ) as sub_start_month,
            date_trunc(
                'month',
                dateadd('month', -1, zuora_subscriptions_xf.subscription_end_date::date)
            ) as sub_end_month,
            date_trunc(
                'month', zuora_rpc.effective_start_date::date
            ) as effective_start_month,
            date_trunc(
                'month', dateadd('month', -1, zuora_rpc.effective_end_date::date)
            ) as effective_end_month,
            datediff(
                month,
                zuora_rpc.effective_start_date::date,
                zuora_rpc.effective_end_date::date
            ) as month_interval,
            zuora_rpc.effective_start_date,
            zuora_rpc.effective_end_date,
            original_rate_plan_id.subscription_version_term_start_date,
            original_rate_plan_id.subscription_version_term_end_date,
            zuora_subscriptions_xf.cohort_month,
            zuora_subscriptions_xf.cohort_quarter
        from zuora_accts
        inner join
            zuora_subscriptions_xf
            on zuora_accts.account_id = zuora_subscriptions_xf.account_id
        inner join
            zuora_rp
            on zuora_rp.subscription_id = zuora_subscriptions_xf.subscription_id
        inner join
            zuora_rpc
            on zuora_rpc.rate_plan_id = zuora_rp.rate_plan_id
            and zuora_rpc.mrr > 0
            and zuora_rpc.tcv > 0
        left join
            zuora_contact
            on coalesce(zuora_accts.sold_to_contact_id, zuora_accts.bill_to_contact_id)
            = zuora_contact.contact_id
        left join
            original_rate_plan_id
            on zuora_rpc.original_id = original_rate_plan_id.original_id

    )

select *
from base_mrr
where effective_end_month >= effective_start_month
