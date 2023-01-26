with
    zuora_accts as (select * from {{ ref("zuora_account") }}),
    zuora_subs as (select * from {{ ref("zuora_subscription") }}),
    zuora_rp as (select * from {{ ref("zuora_rate_plan") }}),
    zuora_rpc as (select * from {{ ref("zuora_rate_plan_charge") }}),
    zuora_i as (

        select *
        from {{ ref("zuora_invoice") }}
        where
            status
            = 'Posted'  -- posted is important!

    ),
    zuora_ii as (

        select
            *,
            date_trunc('month', service_start_date)::date
            as service_month  -- use current month
        from {{ ref("zuora_invoice_item") }}

    ),
    sub_months as (

        select
            country,
            account_number,
            cohort_month,
            cohort_quarter,
            subscription_name,
            subscription_name_slugify,
            oldest_subscription_in_cohort,
            lineage
        from {{ ref("zuora_base_mrr") }} {{ dbt_utils.group_by(n=8) }}

    ),
    charges as (

        select s.subscription_name, s.subscription_name_slugify, ii.*
        from zuora_ii ii
        inner join zuora_i i on i.invoice_id = ii.invoice_id
        left join zuora_rpc rpc on rpc.rate_plan_charge_id = ii.rate_plan_charge_id
        left join zuora_rp rp on rpc.rate_plan_id = rp.rate_plan_id
        left join zuora_subs s on rp.subscription_id = s.subscription_id
    )

select

    sub_months.*,
    charges.service_month,
    {{
        dbt_utils.star(
            from=ref("zuora_invoice_item"),
            except=["SUBSCRIPTION_NAME", "SUBSCRIPTION_NAME_SLUGIFY"],
        )
    }}
from charges
left join
    sub_months
    on charges.subscription_name_slugify = sub_months.subscription_name_slugify
where sub_months.cohort_month is not null
