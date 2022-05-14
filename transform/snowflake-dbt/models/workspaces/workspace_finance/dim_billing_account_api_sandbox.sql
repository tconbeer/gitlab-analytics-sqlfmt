{{ config(tags=["mnpi_exception"]) }}

{{
    simple_cte(
        [
            ("map_merged_crm_account", "map_merged_crm_account"),
            ("zuora_api_sandbox_contact", "zuora_api_sandbox_contact_source"),
        ]
    )
}}

,
zuora_api_sandbox_account as (

    select *
    from {{ ref("zuora_api_sandbox_account_source") }}
    -- Exclude Batch20 which are the test accounts. This method replaces the manual
    -- dbt seed exclusion file.
    where lower(batch) != 'batch20' and is_deleted = false

),
filtered as (

    select
        zuora_api_sandbox_account.account_id as dim_billing_account_id,
        map_merged_crm_account.dim_crm_account_id as dim_crm_account_id,
        zuora_api_sandbox_account.account_number as billing_account_number,
        zuora_api_sandbox_account.account_name as billing_account_name,
        zuora_api_sandbox_account.status as account_status,
        zuora_api_sandbox_account.parent_id,
        zuora_api_sandbox_account.sfdc_account_code,
        zuora_api_sandbox_account.currency as account_currency,
        zuora_api_sandbox_contact.country as sold_to_country,
        zuora_api_sandbox_account.is_deleted,
        zuora_api_sandbox_account.batch
    from zuora_api_sandbox_account
    left join
        zuora_api_sandbox_contact on coalesce(
            zuora_api_sandbox_account.sold_to_contact_id,
            zuora_api_sandbox_account.bill_to_contact_id
        ) = zuora_api_sandbox_contact.contact_id
    left join
        map_merged_crm_account
        on zuora_api_sandbox_account.crm_id = map_merged_crm_account.sfdc_account_id

)

{{
    dbt_audit(
        cte_ref="filtered",
        created_by="@ken_aguilar",
        updated_by="@ken_aguilar",
        created_date="2021-08-25",
        updated_date="2021-08-25",
    )
}}
