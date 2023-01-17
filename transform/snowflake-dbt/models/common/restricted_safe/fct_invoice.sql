with
    crm_account_dimensions as (select * from {{ ref("map_crm_account") }}),
    map_merged_crm_account as (select * from {{ ref("map_merged_crm_account") }}),
    zuora_account as (

        select * from {{ ref("zuora_account_source") }} where is_deleted = false

    ),
    zuora_invoice as (

        select * from {{ ref("zuora_invoice_source") }} where is_deleted = false

    ),
    final_invoice as (

        select
            -- ids
            zuora_invoice.invoice_id as dim_invoice_id,

            -- shared dimension keys
            zuora_invoice.account_id as dim_billing_account_id,
            map_merged_crm_account.dim_crm_account_id as dim_crm_account_id,
            crm_account_dimensions.dim_parent_crm_account_id,
            crm_account_dimensions.dim_parent_sales_segment_id,
            crm_account_dimensions.dim_parent_sales_territory_id,
            crm_account_dimensions.dim_parent_industry_id,
            crm_account_dimensions.dim_parent_location_country_id,
            crm_account_dimensions.dim_parent_location_region_id,
            crm_account_dimensions.dim_account_sales_segment_id,
            crm_account_dimensions.dim_account_sales_territory_id,
            crm_account_dimensions.dim_account_industry_id,
            crm_account_dimensions.dim_account_location_country_id,
            crm_account_dimensions.dim_account_location_region_id,

            -- invoice dates
            {{ get_date_id("zuora_invoice.invoice_date") }} as invoice_date_id,
            {{ get_date_id("zuora_invoice.created_date") }} as created_date_id,
            {{ get_date_id("zuora_invoice.due_date") }} as due_date_id,
            {{ get_date_id("zuora_invoice.posted_date") }} as posted_date_id,
            {{ get_date_id("zuora_invoice.target_date") }} as target_date_id,

            -- invoice flags
            zuora_invoice.includes_one_time,
            zuora_invoice.includesrecurring,
            zuora_invoice.includes_usage,
            zuora_invoice.transferred_to_accounting,

            -- additive fields
            zuora_invoice.adjustment_amount,
            zuora_invoice.amount,
            zuora_invoice.amount_without_tax,
            zuora_invoice.balance,
            zuora_invoice.credit_balance_adjustment_amount,
            zuora_invoice.payment_amount,
            zuora_invoice.refund_amount,
            zuora_invoice.tax_amount,
            zuora_invoice.tax_exempt_amount,

            -- metadata
            zuora_invoice.created_by_id,
            zuora_invoice.updated_by_id,
            {{ get_date_id("zuora_invoice.updated_date") }} as updated_date_id

        from zuora_invoice
        inner join zuora_account on zuora_invoice.account_id = zuora_account.account_id
        left join
            map_merged_crm_account
            on zuora_account.crm_id = map_merged_crm_account.sfdc_account_id
        left join
            crm_account_dimensions
            on map_merged_crm_account.dim_crm_account_id
            = crm_account_dimensions.dim_crm_account_id
    )

    {{
        dbt_audit(
            cte_ref="final_invoice",
            created_by="@mcooperDD",
            updated_by="@paul_armstrong",
            created_date="2021-01-20",
            updated_date="2021-04-26",
        )
    }}
