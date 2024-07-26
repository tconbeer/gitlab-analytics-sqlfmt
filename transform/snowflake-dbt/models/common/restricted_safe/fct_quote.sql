with
    crm_account as (select * from {{ ref("map_crm_account") }}),
    invoice as (

        select * from {{ ref("zuora_invoice_source") }} where is_deleted = 'FALSE'

    ),
    opportunity_dimensions as (select * from {{ ref("map_crm_opportunity") }}),
    quote as (

        select * from {{ ref("sfdc_zqu_quote_source") }} where is_deleted = 'FALSE'

    ),
    final_quotes as (

        select

            -- ids
            quote.zqu_quote_id as dim_quote_id,
            quote.zqu__account as dim_crm_account_id,
            crm_account.dim_parent_crm_account_id,
            quote.zqu__zuora_account_id as dim_billing_account_id,

            -- shared dimension keys
            quote.zqu__opportunity as dim_crm_opportunity_id,
            quote.zqu__zuora_subscription_id as dim_subscription_id,
            quote.owner_id as dim_crm_user_id,
            opportunity_dimensions.dim_crm_user_id as opp_dim_crm_user_id,
            opportunity_dimensions.dim_order_type_id as opp_dim_order_type_id,
            opportunity_dimensions.dim_sales_qualified_source_id
            as opp_dim_sales_qualified_source_id,
            opportunity_dimensions.dim_deal_path_id as opp_dim_deal_path_id,
            crm_account.dim_parent_sales_segment_id,
            crm_account.dim_parent_sales_territory_id,
            crm_account.dim_parent_industry_id,
            crm_account.dim_parent_location_country_id,
            crm_account.dim_parent_location_region_id,
            crm_account.dim_account_sales_segment_id,
            crm_account.dim_account_sales_territory_id,
            crm_account.dim_account_industry_id,
            crm_account.dim_account_location_country_id,
            crm_account.dim_account_location_region_id,
            invoice.invoice_id as dim_invoice_id,

            -- dates
            quote.created_date,
            quote.quote_end_date,
            quote.zqu__valid_until as quote_valid_until

        from quote
        left join
            opportunity_dimensions
            on quote.zqu__opportunity = opportunity_dimensions.dim_crm_opportunity_id
        left join invoice on quote.invoice_number = invoice.invoice_number
        left join crm_account on quote.zqu__account = crm_account.dim_crm_account_id

    )

    {{
        dbt_audit(
            cte_ref="final_quotes",
            created_by="@mcooperDD",
            updated_by="@mcooperDD",
            created_date="2021-01-11",
            updated_date="2021-03-04",
        )
    }}
