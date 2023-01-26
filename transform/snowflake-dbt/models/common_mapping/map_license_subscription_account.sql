{{ config(tags=["mnpi_exception"]) }}

{{
    config(
        {
            "materialized": "view",
        }
    )
}}

with
    license as (

        select dim_license_id, license_md5, dim_subscription_id
        from {{ ref("dim_license") }}

    ),
    subscription as (

        select dim_subscription_id, dim_crm_account_id
        from {{ ref("prep_subscription") }}

    ),
    crm_account as (

        select dim_crm_account_id, dim_parent_crm_account_id
        from {{ ref("dim_crm_account") }}

    ),
    license_mapped_to_subscription as (

        select
            license.dim_license_id,
            license.license_md5,
            subscription.dim_subscription_id,
            subscription.dim_crm_account_id,
            iff(license.dim_subscription_id is not null, true, false)
            -- does the license table have a value in both license_id and
            -- subscription_id
            as is_license_mapped_to_subscription,
            iff(subscription.dim_subscription_id is null, false, true)
            -- is the subscription_id in the license table valid (does it exist in the
            -- dim_subscription table?)
            as is_license_subscription_id_valid
        from license
        left join
            subscription
            on license.dim_subscription_id = subscription.dim_subscription_id

    ),
    subscription_mapped_to_crm_account as (

        select
            subscription.dim_subscription_id,
            subscription.dim_crm_account_id,
            crm_account.dim_parent_crm_account_id
        from subscription
        inner join
            crm_account
            on subscription.dim_crm_account_id = crm_account.dim_crm_account_id

    ),
    joined as (

        select
            license_mapped_to_subscription.dim_license_id,
            license_mapped_to_subscription.license_md5,
            license_mapped_to_subscription.is_license_mapped_to_subscription,
            license_mapped_to_subscription.is_license_subscription_id_valid,
            license_mapped_to_subscription.dim_subscription_id,
            license_mapped_to_subscription.dim_crm_account_id,
            subscription_mapped_to_crm_account.dim_parent_crm_account_id
        from license_mapped_to_subscription
        inner join
            subscription_mapped_to_crm_account
            on license_mapped_to_subscription.dim_subscription_id
            = subscription_mapped_to_crm_account.dim_subscription_id

    ),
    license_statistics as (

        select
            dim_license_id,
            count(distinct license_md5) as total_number_md5_per_license,
            count(
                distinct dim_subscription_id
            ) as total_number_subscription_per_license,
            count(distinct dim_crm_account_id) as total_number_crm_account_per_license,
            count(
                distinct dim_parent_crm_account_id
            ) as total_number_ultimate_parent_account_per_license
        from joined
        group by 1

    ),
    final as (

        select
            joined.dim_license_id,
            joined.license_md5,
            joined.is_license_mapped_to_subscription,
            joined.is_license_subscription_id_valid,
            joined.dim_subscription_id,
            joined.dim_crm_account_id,
            joined.dim_parent_crm_account_id,
            license_statistics.total_number_md5_per_license,
            license_statistics.total_number_subscription_per_license,
            license_statistics.total_number_crm_account_per_license,
            license_statistics.total_number_ultimate_parent_account_per_license
        from joined
        inner join
            license_statistics
            on joined.dim_license_id = license_statistics.dim_license_id

    )

    {{
        dbt_audit(
            cte_ref="joined",
            created_by="@kathleentam",
            updated_by="@mcooperDD",
            created_date="2021-01-10",
            updated_date="2021-02-17",
        )
    }}
