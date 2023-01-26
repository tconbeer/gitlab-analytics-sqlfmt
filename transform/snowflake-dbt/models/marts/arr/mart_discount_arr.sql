with
    dim_date as (select * from {{ ref("dim_date") }}),
    dim_billing_account as (select * from {{ ref("dim_billing_account") }}),
    dim_crm_account as (select * from {{ ref("dim_crm_account") }}),
    dim_product_detail as (select * from {{ ref("dim_product_detail") }}),
    fct_invoice_item as (

        select *
        from {{ ref("fct_invoice_item") }}
        where is_last_segment_version = true and arr != 0

    ),
    zuora_subscription as (

        select *
        from {{ ref("zuora_subscription_source") }}
        where is_deleted = false and exclude_from_analysis in ('False', '')

    ),
    dim_charge as (select * from {{ ref("dim_charge") }}),
    arr_agg as (

        select
            fct_invoice_item.charge_id as dim_charge_id,
            fct_invoice_item.effective_start_month,
            fct_invoice_item.effective_end_month,
            dim_billing_account_id_subscription,
            dim_crm_account_id_subscription,
            dim_billing_account_id_invoice,
            dim_crm_account_id_invoice,
            dim_subscription_id,
            dim_product_detail_id,
            is_paid_in_full,
            sum(invoice_item_charge_amount) as invoice_item_charge_amount,
            sum(mrr) as mrr,
            sum(arr) as arr,
            sum(quantity) as quantity
        from fct_invoice_item
        left join dim_charge on dim_charge.dim_charge_id = fct_invoice_item.charge_id
        where
            fct_invoice_item.effective_end_month
            > fct_invoice_item.effective_start_month
            or fct_invoice_item.effective_end_month is null
            -- filter out 2 subscription_ids with known data quality issues when
            -- comparing invoiced subscriptions to the Zuora UI.
            and dim_subscription_id not in (
                '2c92a0ff5e1dcf14015e3c191d4f7689', '2c92a00e6a3477b5016a46aaec2f08bc'
            )
            {{ dbt_utils.group_by(n=10) }}

    ),
    combined as (

        select
            {{ dbt_utils.surrogate_key(["arr_agg.dim_charge_id"]) }} as primary_key,
            arr_agg.dim_charge_id,
            arr_agg.dim_subscription_id,
            arr_agg.effective_start_month,
            arr_agg.effective_end_month,
            date_trunc(
                'month', zuora_subscription.subscription_start_date
            ) as subscription_start_month,
            date_trunc(
                'month', zuora_subscription.subscription_end_date
            ) as subscription_end_month,
            zuora_subscription.crm_opportunity_name,
            dim_crm_account_invoice.dim_parent_crm_account_id
            as dim_parent_crm_account_id_invoice,
            dim_crm_account_invoice.parent_crm_account_name
            as parent_crm_account_name_invoice,
            dim_crm_account_invoice.parent_crm_account_billing_country
            as parent_crm_account_billing_country_invoice,
            dim_crm_account_invoice.parent_crm_account_sales_segment
            as parent_crm_account_sales_segment_invoice,
            dim_crm_account_invoice.dim_crm_account_id as dim_crm_account_id_invoice,
            dim_crm_account_invoice.crm_account_name as crm_account_name_invoice,
            dim_crm_account_invoice.crm_account_owner_team
            as crm_account_owner_team_invoice,
            dim_crm_account_subscription.dim_parent_crm_account_id
            as dim_parent_crm_account_id_subscription,
            dim_crm_account_subscription.parent_crm_account_name
            as parent_crm_account_name_subscription,
            dim_crm_account_subscription.parent_crm_account_billing_country
            as parent_crm_account_billing_country_subscription,
            dim_crm_account_subscription.parent_crm_account_sales_segment
            as parent_crm_account_sales_segment_subscription,
            dim_crm_account_subscription.dim_crm_account_id
            as dim_crm_account_id_subscription,
            dim_crm_account_subscription.crm_account_name
            as crm_account_name_subscription,
            dim_crm_account_subscription.crm_account_owner_team
            as crm_account_owner_team_subscription,
            zuora_subscription.subscription_name,
            iff(
                zuora_subscription.zuora_renewal_subscription_name != '', true, false
            ) as is_myb,
            arr_agg.is_paid_in_full,
            zuora_subscription.current_term as current_term_months,
            round(zuora_subscription.current_term / 12, 1) as current_term_years,
            dim_crm_account_invoice.is_reseller,
            dim_product_detail.product_rate_plan_charge_name,
            dim_product_detail.product_tier_name as product_category,
            dim_product_detail.product_delivery_type as delivery,
            dim_product_detail.service_type,
            case
                when
                    lower(dim_product_detail.product_rate_plan_charge_name)
                    like '%edu or oss%'
                then true
                when
                    lower(dim_product_detail.product_rate_plan_charge_name)
                    like '%education%'
                then true
                when
                    lower(dim_product_detail.product_rate_plan_charge_name)
                    like '%y combinator%'
                then true
                when
                    lower(dim_product_detail.product_rate_plan_charge_name)
                    like '%support%'
                then true
                when
                    lower(dim_product_detail.product_rate_plan_charge_name)
                    like '%reporter%'
                then true
                when
                    lower(dim_product_detail.product_rate_plan_charge_name)
                    like '%guest%'
                then true
                when crm_opportunity_name like '%EDU%'
                then true
                when dim_product_detail.annual_billing_list_price = 0
                then true
                else false
            end as is_excluded_from_disc_analysis,
            dim_product_detail.annual_billing_list_price,
            array_agg(
                iff(
                    zuora_subscription.created_by_id
                    = '2c92a0fd55822b4d015593ac264767f2',  -- All Self-Service / Web direct subscriptions are identified by that created_by_id
                    'Self-Service',
                    'Sales-Assisted'
                )
            ) as subscription_sales_type,
            sum(arr_agg.invoice_item_charge_amount) as invoice_item_charge_amount,
            sum(arr_agg.arr) / sum(arr_agg.quantity) as arpu,
            sum(arr_agg.arr) as arr,
            sum(arr_agg.quantity) as quantity
        from arr_agg
        inner join
            zuora_subscription
            on arr_agg.dim_subscription_id = zuora_subscription.subscription_id
        inner join
            dim_product_detail
            on arr_agg.dim_product_detail_id = dim_product_detail.dim_product_detail_id
        inner join
            dim_billing_account
            on arr_agg.dim_billing_account_id_invoice
            = dim_billing_account.dim_billing_account_id
        left join
            dim_crm_account as dim_crm_account_invoice
            on arr_agg.dim_crm_account_id_invoice
            = dim_crm_account_invoice.dim_crm_account_id
        left join
            dim_crm_account as dim_crm_account_subscription
            on arr_agg.dim_crm_account_id_subscription
            = dim_crm_account_subscription.dim_crm_account_id
        where
            dim_crm_account_subscription.is_jihu_account != 'TRUE'
            {{ dbt_utils.group_by(n=34) }}
        order by 3 desc

    ),
    final as (

        select
            combined.*,
            abs(invoice_item_charge_amount)
            / (arr * current_term_years) as pct_paid_of_total_revenue,
            {{
                arr_buckets(
                    "SUM(arr) OVER(PARTITION BY dim_parent_crm_account_id_invoice,         effective_start_month, effective_end_month, subscription_name,         product_rate_plan_charge_name)"
                )
            }}
            as arr_buckets,
            {{
                number_of_seats_buckets(
                    "SUM(quantity) OVER(PARTITION BY dim_parent_crm_account_id_invoice,         effective_start_month, effective_end_month, subscription_name,         product_rate_plan_charge_name)"
                )
            }}
            as number_of_seats_buckets
        from combined

    )

    {{
        dbt_audit(
            cte_ref="final",
            created_by="@iweeks",
            updated_by="@iweeks",
            created_date="2020-10-21",
            updated_date="2021-10-25",
        )
    }}
