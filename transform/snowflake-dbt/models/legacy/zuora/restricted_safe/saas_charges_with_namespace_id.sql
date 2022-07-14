with
    zuora_base_mrr as (

        select * from {{ ref("zuora_base_mrr") }} where delivery = 'SaaS'),
    customers_db_charges as (select * from {{ ref("customers_db_charges_xf") }}),
    namespaces as (select * from {{ ref("gitlab_dotcom_namespaces") }}),
    dim_billing_account as (select * from {{ ref("dim_billing_account") }}),
    dim_crm_account as (select * from {{ ref("dim_crm_account") }}),
    dim_subscription as (select * from {{ ref("dim_subscription") }}),
    joined as (

        select
            zuora_base_mrr.rate_plan_charge_id,
            zuora_base_mrr.subscription_name_slugify,
            dim_billing_account.dim_billing_account_id as dim_billing_account_id,
            coalesce(
                merged_accounts.dim_crm_account_id, dim_crm_account.dim_crm_account_id
            ) as dim_crm_account_id,
            coalesce(
                merged_accounts.dim_parent_crm_account_id,
                dim_crm_account.dim_parent_crm_account_id
            ) as dim_parent_crm_account_id,
            coalesce(
                merged_accounts.parent_crm_account_name,
                dim_crm_account.parent_crm_account_name
            ) as parent_crm_account_name,
            customers_db_charges.current_customer_id,
            namespaces.namespace_id
        from zuora_base_mrr
        left join
            customers_db_charges
            on zuora_base_mrr.rate_plan_charge_id
            = customers_db_charges.rate_plan_charge_id
        left join
            namespaces
            on customers_db_charges.current_gitlab_namespace_id
            = namespaces.namespace_id
        left join
            dim_billing_account
            on zuora_base_mrr.account_number
            = dim_billing_account.billing_account_number
        left join
            dim_crm_account
            on dim_billing_account.dim_crm_account_id
            = dim_crm_account.dim_crm_account_id
        left join
            dim_crm_account as merged_accounts
            on dim_crm_account.merged_to_account_id = merged_accounts.dim_crm_account_id

    )

select *
from joined
