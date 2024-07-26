with
    date_table as (select * from {{ ref("date_details") }} where day_of_month = 1),
    zuora_accts as (

        select * from {{ ref("zuora_account_source") }} where is_deleted = false

    ),
    zuora_acct_period as (select * from {{ ref("zuora_accounting_period_source") }}),
    zuora_contact as (

        select * from {{ ref("zuora_contact_source") }} where is_deleted = false

    ),
    zuora_product as (

        select * from {{ ref("zuora_product_source") }} where is_deleted = false

    ),
    zuora_rp as (

        select * from {{ ref("zuora_rate_plan_source") }} where is_deleted = false

    ),
    zuora_rpc as (

        select *
        from {{ ref("zuora_rate_plan_charge_source") }}
        where is_deleted = false

    ),
    zuora_subscription as (

        select *
        from {{ ref("zuora_subscription_source") }}
        where is_deleted = false and exclude_from_analysis in ('False', '')

    ),
    base_mrr as (

        select
            -- primary key
            zuora_rpc.rate_plan_charge_id,

            -- account info
            zuora_accts.account_id,
            zuora_accts.account_name,
            zuora_accts.account_number,
            zuora_accts.crm_id,
            zuora_contact.country,
            zuora_accts.currency,

            -- subscription info
            zuora_subscription.subscription_id,
            zuora_subscription.subscription_name_slugify,

            -- rate_plan info
            zuora_rpc.product_rate_plan_charge_id,
            zuora_rp.rate_plan_name,
            zuora_rpc.rate_plan_charge_name,
            zuora_rpc.rate_plan_charge_number,
            zuora_rpc.unit_of_measure,
            zuora_rpc.quantity,
            zuora_rpc.mrr,
            zuora_rpc.charge_type,

            -- date info
            date_trunc(
                'month', zuora_subscription.subscription_start_date::date
            ) as sub_start_month,
            date_trunc(
                'month', zuora_subscription.subscription_end_date::date
            ) as sub_end_month,
            subscription_start_date::date as subscription_start_date,
            subscription_end_date::date as subscription_end_date,
            zuora_rpc.effective_start_month,
            zuora_rpc.effective_end_month,
            zuora_rpc.effective_start_date::date as effective_start_date,
            zuora_rpc.effective_end_date::date as effective_end_date
        from zuora_accts
        inner join
            zuora_subscription on zuora_accts.account_id = zuora_subscription.account_id
        inner join
            zuora_rp on zuora_rp.subscription_id = zuora_subscription.subscription_id
        inner join zuora_rpc on zuora_rpc.rate_plan_id = zuora_rp.rate_plan_id
        left join
            zuora_contact
            on coalesce(zuora_accts.sold_to_contact_id, zuora_accts.bill_to_contact_id)
            = zuora_contact.contact_id
        left join zuora_product on zuora_product.product_id = zuora_rpc.product_id
        where
            zuora_subscription.subscription_status not in ('Draft', 'Expired')
            and zuora_rpc.charge_type = 'Recurring'
            and mrr != 0

    ),
    month_base_mrr as (

        select
            date_actual as mrr_month,
            account_number,
            crm_id,
            account_name,
            account_id,
            subscription_id,
            subscription_name_slugify,
            sub_start_month,
            sub_end_month,
            subscription_start_date,
            subscription_end_date,
            effective_start_month,
            effective_end_month,
            effective_start_date,
            effective_end_date,
            country,
            {{ product_category("rate_plan_name") }},
            {{ delivery("product_category") }},
            case
                when lower(rate_plan_name) like '%support%'
                then 'Support Only'
                else 'Full Service'
            end as service_type,
            product_rate_plan_charge_id,
            rate_plan_name,
            rate_plan_charge_name,
            charge_type,
            unit_of_measure,
            sum(mrr) as mrr,
            sum(quantity) as quantity
        from base_mrr
        inner join
            date_table
            on base_mrr.effective_start_month <= date_table.date_actual
            and (
                base_mrr.effective_end_month > date_table.date_actual
                or base_mrr.effective_end_month is null
            )
            {{ dbt_utils.group_by(n=24) }}

    ),
    current_mrr as (

        select
            zuora_accts.account_id,
            zuora_subscription.subscription_id,
            zuora_subscription.subscription_name_slugify,
            sum(zuora_rpc.mrr) as total_current_mrr
        from zuora_accts
        inner join
            zuora_subscription on zuora_accts.account_id = zuora_subscription.account_id
        inner join
            zuora_rp on zuora_rp.subscription_id = zuora_subscription.subscription_id
        inner join zuora_rpc on zuora_rpc.rate_plan_id = zuora_rp.rate_plan_id
        where
            zuora_subscription.subscription_status not in ('Draft', 'Expired')
            and effective_start_date <= current_date
            and (effective_end_date > current_date or effective_end_date is null)
            {{ dbt_utils.group_by(n=3) }}

    )

select
    mrr_month,
    month_base_mrr.account_id,
    account_number,
    account_name,
    crm_id,
    month_base_mrr.subscription_id,
    month_base_mrr.subscription_name_slugify,
    sub_start_month,
    sub_end_month,
    effective_start_month,
    effective_end_month,
    country,
    product_category,
    delivery,
    service_type,
    product_rate_plan_charge_id,
    rate_plan_name,
    rate_plan_charge_name,
    charge_type,
    unit_of_measure,
    sum(mrr) as mrr,
    sum(mrr * 12) as arr,
    sum(quantity) as quantity,
    max(total_current_mrr) as total_current_mrr
from month_base_mrr
left join
    current_mrr on month_base_mrr.subscription_id = current_mrr.subscription_id
    {{ dbt_utils.group_by(n=20) }}
