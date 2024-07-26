with
    crm_account as (select * from {{ ref("map_crm_account") }}),
    invoice as (select * from {{ ref("zuora_invoice_source") }}),
    opp as (

        select * from {{ ref("sfdc_opportunity_source") }} where is_deleted = 'FALSE'

    ),
    opp_relational_fields as (select * from {{ ref("map_crm_opportunity") }}),
    quote as (

        select * from {{ ref("sfdc_zqu_quote_source") }} where is_deleted = 'FALSE'

    ),
    quote_amendment as (

        select *
        from {{ ref("sfdc_zqu_quote_amendment_source") }}
        where is_deleted = 'FALSE'

    ),
    rate_plan as (

        select *
        from {{ ref("sfdc_zqu_quote_rate_plan_source") }}
        where is_deleted = 'FALSE'

    ),
    rate_plan_charge as (

        select *
        from {{ ref("sfdc_zqu_quote_rate_plan_charge_source") }}
        where is_deleted = 'FALSE'

    ),
    quote_items as (

        select

            -- ids
            {{
                dbt_utils.surrogate_key(
                    [
                        "quote_amendment.zqu_quote_amendment_id",
                        "COALESCE(rate_plan_charge.zqu_quote_rate_plan_charge_id, MD5(-1))",
                        "COALESCE(rate_plan_charge.zqu_product_rate_plan_charge_zuora_id, MD5(-1))",
                    ]
                )
            }}
            as quote_item_id,
            quote_amendment.zqu_quote_amendment_id as quote_amendment_id,
            quote.quote_id as dim_quote_id,
            quote.owner_id as dim_crm_user_id,

            -- relational keys
            quote.zqu__account as dim_crm_account_id,
            crm_account.dim_parent_crm_account_id,
            quote.zqu__zuora_account_id as dim_billing_account_id,
            quote.zqu__zuora_subscription_id as dim_subscription_id,
            opp.opportunity_id as dim_crm_opportunity_id,
            opp_relational_fields.dim_crm_user_id as opp_dim_crm_user_id,
            opp_relational_fields.dim_order_type_id as opp_dim_order_type_id,
            opp_relational_fields.dim_sales_qualified_source_id
            as opp_dim_sales_qualified_source_id,
            opp_relational_fields.dim_deal_path_id as opp_dim_deal_path_id,
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
            rate_plan.zqu_subscription_rate_plan_zuora_id as rate_plan_id,
            rate_plan.zqu_product_rate_plan_zuora_id as product_rate_plan_id,
            rate_plan_charge.zqu_subscription_rate_plan_charge_zuora_id
            as rate_plan_charge_id,
            rate_plan_charge.zqu_quote_rate_plan_charge_id as quote_rate_plan_charge_id,
            rate_plan_charge.zqu_product_rate_plan_charge_zuora_id
            as dim_product_detail_id,

            -- additive fields
            quote_amendment.zqu__total_amount as total_amount,
            quote_amendment.license_amount as license_amount,
            quote_amendment.professional_services_amount
            as professional_services_amount,
            quote_amendment.true_up_amount as true_up_amount,
            quote_amendment.zqu__delta_mrr as delta_mrr,
            quote_amendment.zqu__delta_tcv as delta_tcv,
            rate_plan_charge.zqu_mrr as mrr,
            rate_plan_charge.zqu_mrr * 12 as arr,
            rate_plan_charge.zqu_tcv as tcv,
            rate_plan_charge.zqu_quantity as quantity

        from quote_amendment
        inner join quote on quote_amendment.zqu__quote = quote.zqu_quote_id
        inner join opp on quote.zqu__opportunity = opp.opportunity_id
        inner join
            opp_relational_fields
            on opp.opportunity_id = opp_relational_fields.dim_crm_opportunity_id
        left join invoice on opp.invoice_number = invoice.invoice_number
        inner join
            rate_plan
            on quote_amendment.zqu_quote_amendment_id = rate_plan.zqu_quote_amendment_id
        inner join
            rate_plan_charge
            on rate_plan.zqu_quote_rate_plan_id
            = rate_plan_charge.zqu_quote_rate_plan_id
        left join crm_account on quote.zqu__account = crm_account.dim_crm_account_id

    )

    {{
        dbt_audit(
            cte_ref="quote_items",
            created_by="@mcooperDD",
            updated_by="@jpeguero",
            created_date="2021-01-12",
            updated_date="2021-10-28",
        )
    }}
