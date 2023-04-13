{{ config(tags=["mnpi_exception"]) }}

{{
    simple_cte(
        [
            ("dim_amendment", "dim_amendment"),
            ("fct_mrr", "fct_mrr"),
            ("dim_product_detail", "dim_product_detail"),
            ("dim_crm_account", "dim_crm_account"),
            ("mart_arr", "mart_arr"),
        ]
    )
}},
dim_subscription as (  -- Add a flag to dim_subscription which specify if the subscription is the last version

    select
        iff(
            max(subscription_version) over (partition by subscription_name)
            = subscription_version,
            true,
            false
        ) as is_last_subscription_version,
        last_value(subscription_start_date) over (
            partition by subscription_name order by subscription_version
        ) as last_subscription_start_date,
        last_value(subscription_end_date) over (
            partition by subscription_name order by subscription_version
        ) as last_subscription_end_date,
        *
    from {{ ref("dim_subscription") }}

),
dim_license as (
    -- Dedup multiple subscription_ids in dim_license. In case of duplicate
    -- subscription_ids first take the one in customers portal and then the one with
    -- the latest license_expire_date
    select *, iff(environment like 'Customer%Portal', 1, 2) as environment_order
    from {{ ref("dim_license") }}
    qualify
        row_number() over (
            partition by dim_subscription_id
            order by environment_order, license_expire_date desc
        )
        = 1
    order by dim_subscription_id

),
subscription_amendments_issue_license_mapping as (

    select
        dim_subscription.subscription_name,
        iff(
            max(
                iff(
                    amendment_type
                    in ('NewProduct', 'RemoveProduct', 'UpdateProduct', 'Renewal'),
                    1,
                    0
                )
            )
            = 1,
            true,
            false
        ) does_subscription_name_contains_amendments_issue_license
    from dim_subscription
    left join
        dim_amendment
        on dim_amendment.dim_amendment_id
        = dim_subscription.dim_amendment_id_subscription
    group by 1

),
subscription_renewal_mapping as (

    select distinct
        subscription_name,
        iff(
            len(trim(zuora_renewal_subscription_name)) = 0,
            null,
            zuora_renewal_subscription_name
        ) as zuora_renewal_subscription_name
    from dim_subscription
    where is_last_subscription_version

),
amendments as (  -- Get subscriptions versions that are the product of the amendments listed in the WHERE clause
    -- These amendments are the ones that should have a license attached to them
    -- In the qualify statement, we get only the latest version that is part of the
    -- amendment list, since the latest one is the one we care about
    select distinct
        dim_subscription.dim_subscription_id,
        dim_subscription.dim_crm_account_id,
        dim_amendment.dim_amendment_id,
        dim_amendment.amendment_name,
        dim_subscription.subscription_version,
        dim_subscription.subscription_status,
        dim_subscription.subscription_start_date,
        dim_subscription.subscription_end_date,
        dim_amendment.amendment_type,
        dim_subscription.subscription_name,
        dim_subscription.dim_billing_account_id_invoice_owner,
        dim_subscription.last_subscription_start_date,
        dim_subscription.last_subscription_end_date,
        subscription_renewal_mapping.zuora_renewal_subscription_name,
        subscription_amendments_issue_license_mapping.does_subscription_name_contains_amendments_issue_license,
        dim_subscription.dbt_updated_at

    from dim_subscription
    left join
        dim_amendment
        on dim_amendment.dim_amendment_id
        = dim_subscription.dim_amendment_id_subscription
    left join
        subscription_renewal_mapping
        on subscription_renewal_mapping.subscription_name
        = dim_subscription.subscription_name
    left join
        subscription_amendments_issue_license_mapping
        on dim_subscription.subscription_name
        = subscription_amendments_issue_license_mapping.subscription_name
    where
        (
            amendment_type
            in ('NewProduct', 'RemoveProduct', 'UpdateProduct', 'Renewal')
            or dim_subscription.subscription_version = 1
        )

    -- Gets latest subscription_version where the ammendments above happened
    qualify
        row_number() over (
            partition by dim_subscription.subscription_name
            order by dim_subscription.subscription_version desc
        )
        = 1

),
ammendments_and_last_version as (  -- Pull the latest subscription version and append it to the ammendments found above.
    -- Reason for this is to look for the license id in the last amendment in case it
    -- is not in the past CTE
    select *
    from amendments

    union

    select distinct
        dim_subscription.dim_subscription_id,
        dim_subscription.dim_crm_account_id,
        dim_amendment.dim_amendment_id,
        dim_amendment.amendment_name,
        dim_subscription.subscription_version,
        dim_subscription.subscription_status,
        dim_subscription.subscription_start_date,
        dim_subscription.subscription_end_date,
        dim_amendment.amendment_type,
        dim_subscription.subscription_name,
        dim_subscription.dim_billing_account_id_invoice_owner,
        dim_subscription.last_subscription_start_date,
        dim_subscription.last_subscription_end_date,
        subscription_renewal_mapping.zuora_renewal_subscription_name,
        subscription_amendments_issue_license_mapping.does_subscription_name_contains_amendments_issue_license,
        dim_subscription.dbt_updated_at
    from dim_subscription
    left join
        dim_amendment
        on dim_amendment.dim_amendment_id
        = dim_subscription.dim_amendment_id_subscription
    left join
        subscription_renewal_mapping
        on subscription_renewal_mapping.subscription_name
        = dim_subscription.subscription_name
    left join
        subscription_amendments_issue_license_mapping
        on dim_subscription.subscription_name
        = subscription_amendments_issue_license_mapping.subscription_name
    where is_last_subscription_version

),
self_managed_subscriptions as (  -- Get subscription_id from self managed subscriptions

    select distinct fct_mrr.dim_subscription_id
    from fct_mrr
    left join
        dim_product_detail
        on fct_mrr.dim_product_detail_id = dim_product_detail.dim_product_detail_id
    left join
        dim_subscription
        on fct_mrr.dim_subscription_id = dim_subscription.dim_subscription_id
    where subscription_start_date <= current_date
    qualify
        last_value(dim_product_detail.product_delivery_type) over (
            partition by dim_subscription.subscription_name
            order by dim_subscription.subscription_version, fct_mrr.dim_date_id
        )
        = 'Self-Managed'

),
subscriptions_with_arr_in_current_month as (  -- Get subscriptions names that are currently paying ARR.
    -- If the subscription is not paying ARR no reason to investigate it
    select subscription_name, sum(arr) as arr
    from mart_arr
    where arr > 0 and arr_month = date_trunc('month', current_date)
    group by 1

),
self_managed_amendments as (  -- Filter the amendments / subscription_versions to be of self managed    

    select ammendments_and_last_version.*
    from ammendments_and_last_version
    inner join
        self_managed_subscriptions
        on ammendments_and_last_version.dim_subscription_id
        = self_managed_subscriptions.dim_subscription_id

),
subscription_to_licenses as (  -- Join subscriptions to licenses

    select
        self_managed_amendments.*,
        dim_license.dim_license_id,
        dim_license.license_md5,
        dim_license.dim_environment_id,
        dim_license.environment,
        dim_license.license_plan,
        dim_license.license_start_date,
        dim_license.license_expire_date
    from self_managed_amendments
    left join
        dim_license
        on self_managed_amendments.dim_subscription_id = dim_license.dim_subscription_id
    order by
        self_managed_amendments.subscription_name,
        self_managed_amendments.subscription_version

),
subscription_to_licenses_final as (  -- If the latest subscription version or the amendment from the amendment list has a valid license 

    select *
    from subscription_to_licenses
    qualify
        row_number() over (
            partition by subscription_name
            order by dim_license_id desc nulls last, subscription_version desc
        )
        = 1

),
licenses_missing_subscriptions as (

    select * from subscription_to_licenses_final where dim_license_id is null

),
licenses_with_subscriptions as (

    select * from subscription_to_licenses_final where dim_license_id is not null

),
report as (

    select 'Missing license' as license_status, licenses_missing_subscriptions.*
    from licenses_missing_subscriptions
    left join
        licenses_with_subscriptions
        on licenses_missing_subscriptions.does_subscription_name_contains_amendments_issue_license
        = false
        and licenses_missing_subscriptions.last_subscription_start_date
        = licenses_with_subscriptions.last_subscription_end_date
        and licenses_missing_subscriptions.subscription_name
        = licenses_with_subscriptions.zuora_renewal_subscription_name
        and licenses_missing_subscriptions.dim_billing_account_id_invoice_owner
        != licenses_with_subscriptions.dim_billing_account_id_invoice_owner
    where licenses_with_subscriptions.dim_subscription_id is null

    union

    select
        'Has license' as license_status,
        licenses_missing_subscriptions.dim_subscription_id,
        licenses_missing_subscriptions.dim_crm_account_id,
        licenses_missing_subscriptions.dim_amendment_id,
        licenses_missing_subscriptions.amendment_name,
        licenses_missing_subscriptions.subscription_version,
        licenses_missing_subscriptions.subscription_status,
        licenses_missing_subscriptions.subscription_start_date,
        licenses_missing_subscriptions.subscription_end_date,
        licenses_missing_subscriptions.amendment_type,
        licenses_missing_subscriptions.subscription_name,
        licenses_missing_subscriptions.dim_billing_account_id_invoice_owner,
        licenses_missing_subscriptions.last_subscription_start_date,
        licenses_missing_subscriptions.last_subscription_end_date,
        licenses_missing_subscriptions.zuora_renewal_subscription_name,
        licenses_missing_subscriptions.does_subscription_name_contains_amendments_issue_license,
        licenses_missing_subscriptions.dbt_updated_at,

        licenses_with_subscriptions.dim_license_id,
        licenses_with_subscriptions.license_md5,
        licenses_with_subscriptions.dim_environment_id,
        licenses_with_subscriptions.environment,
        licenses_with_subscriptions.license_plan,
        licenses_with_subscriptions.license_start_date,
        licenses_with_subscriptions.license_expire_date
    from licenses_missing_subscriptions
    left join
        licenses_with_subscriptions
        on licenses_missing_subscriptions.does_subscription_name_contains_amendments_issue_license
        = false
        and licenses_missing_subscriptions.last_subscription_start_date
        = licenses_with_subscriptions.last_subscription_end_date
        and licenses_missing_subscriptions.subscription_name
        = licenses_with_subscriptions.zuora_renewal_subscription_name
        and licenses_missing_subscriptions.dim_billing_account_id_invoice_owner
        != licenses_with_subscriptions.dim_billing_account_id_invoice_owner
    where licenses_with_subscriptions.dim_subscription_id is not null

    union

    select 'Has license' as license_status, *
    from licenses_with_subscriptions

),
final as (

    select report.*
    from report
    inner join
        subscriptions_with_arr_in_current_month
        on subscriptions_with_arr_in_current_month.subscription_name
        = report.subscription_name

)

select *
from final
