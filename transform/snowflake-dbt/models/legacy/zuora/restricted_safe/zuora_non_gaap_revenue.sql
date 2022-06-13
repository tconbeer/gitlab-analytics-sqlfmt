with
    date_table as (select * from {{ ref("date_details") }}),
    zuora_accts as (select * from {{ ref("zuora_account_source") }}),
    zuora_acct_period as (select * from {{ ref("zuora_accounting_period_source") }}),
    zuora_contact as (select * from {{ ref("zuora_contact_source") }}),
    zuora_product as (select * from {{ ref("zuora_product_source") }}),
    zuora_rev_sch as (select * from {{ ref("zuora_revenue_schedule_item_source") }}),
    zuora_rp as (select * from {{ ref("zuora_rate_plan_source") }}),
    zuora_rpc as (select * from {{ ref("zuora_rate_plan_charge_source") }}),
    non_gaap_revenue as (

        select
            zuora_acct_period.accounting_period_start_date::date as accounting_period,

            -- account info
            zuora_accts.account_name,
            zuora_accts.account_number,
            zuora_accts.crm_id,
            zuora_contact.country,
            zuora_accts.currency,

            -- rate_plan info
            zuora_rp.rate_plan_name,
            zuora_rpc.rate_plan_charge_name,
            {{ product_category("rate_plan_name") }},
            {{ delivery("product_category") }},
            zuora_product.product_name,
            sum(zuora_rev_sch.revenue_schedule_item_amount) as revenue_amt
        from zuora_rev_sch
        inner join zuora_accts on zuora_rev_sch.account_id = zuora_accts.account_id
        left join
            zuora_contact on coalesce(
                zuora_accts.sold_to_contact_id, zuora_accts.bill_to_contact_id
            ) = zuora_contact.contact_id
        inner join
            zuora_rpc
            on zuora_rev_sch.rate_plan_charge_id = zuora_rpc.rate_plan_charge_id
        inner join zuora_rp on zuora_rp.rate_plan_id = zuora_rpc.rate_plan_id
        inner join
            zuora_acct_period
            on zuora_acct_period.accounting_period_id
            = zuora_rev_sch.accounting_period_id
        left join
            zuora_product on zuora_product.product_id = zuora_rev_sch.product_id
            {{ dbt_utils.group_by(n=11) }}

    )

select *
from non_gaap_revenue
